use std::{
    cmp::{max, min},
    sync::Arc,
};

use data_types::{CompactionLevel, ParquetFile, TimestampMinMax};
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunk,
};
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, trace};
use parquet_file::storage::ParquetStorage;
use snafu::{ResultExt, Snafu};

use crate::{
    components::compact::query_chunk::{to_queryable_parquet_chunk, QueryableParquetChunk},
    config::Config,
};

use super::partition::PartitionInfo;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error building compact logical plan  {}", source))]
    CompactLogicalPlan {
        source: iox_query::frontend::reorg::Error,
    },

    #[snafu(display("Error building compact physical plan  {}", source))]
    CompactPhysicalPlan { source: DataFusionError },
}

/// Observer function (for testing) that is invoked on the physical plan to be run
type PlanObserver = Box<dyn Fn(&dyn ExecutionPlan) + Send>;

/// Builder for compaction plans
pub(crate) struct CompactPlanBuilder {
    // Partition of files to compact
    partition: Arc<PartitionInfo>,
    files: Arc<Vec<ParquetFile>>,
    store: ParquetStorage,
    exec: Arc<Executor>,
    _time_provider: Arc<dyn TimeProvider>,
    max_desired_file_size_bytes: u64,
    percentage_max_file_size: u16,
    split_percentage: u16,
    target_level: CompactionLevel,
    // This is for plan observation for testing
    plan_observer: Option<PlanObserver>,
}

impl CompactPlanBuilder {
    /// Create a new compact plan builder.
    pub fn new(
        files: Arc<Vec<ParquetFile>>,
        partition: Arc<PartitionInfo>,
        config: Arc<Config>,
        compaction_level: CompactionLevel,
    ) -> Self {
        Self {
            partition,
            files,
            store: config.parquet_store.clone(),
            exec: Arc::clone(&config.exec),
            _time_provider: Arc::clone(&config.time_provider),
            max_desired_file_size_bytes: config.max_desired_file_size_bytes,
            percentage_max_file_size: config.percentage_max_file_size,
            split_percentage: config.split_percentage,
            target_level: compaction_level,
            plan_observer: None,
        }
    }

    /// specify a function to call on the created physical plan, prior to its execution (used for testing)
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn with_plan_observer(mut self, plan_observer: PlanObserver) -> Self {
        self.plan_observer = Some(plan_observer);
        self
    }

    /// Builds a compact plan respecting the specified file boundaries
    /// This functon assumes that the compaction-levels of the files are  either target_level or target_level-1
    pub async fn build_compact_plan(self) -> Result<Arc<dyn ExecutionPlan>, Error> {
        //Result<LogicalPlan, Error> {
        let Self {
            partition,
            files,
            store,
            exec,
            _time_provider,
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage,
            target_level,
            plan_observer,
        } = self;

        // total file size is the sum of the file sizes of the files to compact
        let file_sizes = files.iter().map(|f| f.file_size_bytes).collect::<Vec<_>>();
        let total_size: i64 = file_sizes.iter().sum();
        let total_size = total_size as u64;

        // Convert the input files into QueryableParquetChunk for making query plan
        let query_chunks: Vec<_> = files
            .iter()
            .map(|file| {
                to_queryable_parquet_chunk(
                    file.clone(),
                    store.clone(),
                    &partition.table_schema,
                    partition.sort_key.clone(),
                    target_level,
                )
            })
            .collect();

        trace!(
            n_query_chunks = query_chunks.len(),
            "gathered parquet data to compact"
        );

        // Compute min/max time
        // unwrap here will work because the len of the query_chunks already >= 1
        let (head, tail) = query_chunks.split_first().unwrap();
        let mut min_time = head.min_time();
        let mut max_time = head.max_time();
        for c in tail {
            min_time = min(min_time, c.min_time());
            max_time = max(max_time, c.max_time());
        }

        // extract the min & max chunk times for filtering potential split times.
        let chunk_times: Vec<_> = query_chunks
            .iter()
            .map(|c| TimestampMinMax::new(c.min_time(), c.max_time()))
            .collect();

        // Merge schema of the compacting chunks
        let query_chunks: Vec<_> = query_chunks
            .into_iter()
            .map(|c| Arc::new(c) as Arc<dyn QueryChunk>)
            .collect();
        let merged_schema = QueryableParquetChunk::merge_schemas(&query_chunks);
        debug!(
            num_cols = merged_schema.as_arrow().fields().len(),
            "Number of columns in the merged schema to build query plan"
        );

        // All partitions in the catalog MUST contain a sort key.
        let sort_key = partition
            .sort_key
            .as_ref()
            .expect("no partition sort key in catalog")
            .filter_to(&merged_schema.primary_key(), partition.partition_id.get());

        let (small_cutoff_bytes, large_cutoff_bytes) =
            Self::cutoff_bytes(max_desired_file_size_bytes, percentage_max_file_size);

        // Build logical compact plan
        let ctx = exec.new_context(ExecutorType::Reorg);
        let plan = if total_size <= small_cutoff_bytes {
            // Compact everything into one file
            ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
                .compact_plan(
                    Arc::from(partition.table.name.clone()),
                    &merged_schema,
                    query_chunks,
                    sort_key,
                )
                .context(CompactLogicalPlanSnafu)?
        } else {
            let split_times = if small_cutoff_bytes < total_size && total_size <= large_cutoff_bytes
            {
                // Split compaction into two files, the earlier of split_percentage amount of
                // max_desired_file_size_bytes, the later of the rest
                vec![min_time + ((max_time - min_time) * split_percentage as i64) / 100]
            } else {
                // Split compaction into multiple files
                Self::compute_split_time(
                    chunk_times,
                    min_time,
                    max_time,
                    total_size,
                    max_desired_file_size_bytes,
                )
            };

            if split_times.is_empty() || (split_times.len() == 1 && split_times[0] == max_time) {
                // The split times might not have actually split anything, so in this case, compact
                // everything into one file
                ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
                    .compact_plan(
                        Arc::from(partition.table.name.clone()),
                        &merged_schema,
                        query_chunks,
                        sort_key,
                    )
                    .context(CompactLogicalPlanSnafu)?
            } else {
                // split compact query plan
                ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
                    .split_plan(
                        Arc::from(partition.table.name.clone()),
                        &merged_schema,
                        query_chunks,
                        sort_key,
                        split_times,
                    )
                    .context(CompactLogicalPlanSnafu)?
            }
        };

        // Build physical compact plan
        let physical_plan = ctx
            .create_physical_plan(&plan)
            .await
            .context(CompactPhysicalPlanSnafu)?;

        if let Some(plan_observer) = plan_observer {
            plan_observer(physical_plan.as_ref());
        }

        Ok(physical_plan)
    }

    // compute cut off bytes for files
    fn cutoff_bytes(max_desired_file_size_bytes: u64, percentage_max_file_size: u16) -> (u64, u64) {
        (
            (max_desired_file_size_bytes * percentage_max_file_size as u64) / 100,
            (max_desired_file_size_bytes * (100 + percentage_max_file_size as u64)) / 100,
        )
    }

    // Compute time to split data
    // Return a list of times at which we want data to be split. The times are computed
    // based on the max_desired_file_size each file should not exceed and the total_size this input
    // time range [min_time, max_time] contains.
    // The split times assume that the data is evenly distributed in the time range and if
    // that is not the case the resulting files are not guaranteed to be below max_desired_file_size
    // Hence, the range between two contiguous returned time is percentage of
    // max_desired_file_size/total_size of the time range
    // Example:
    //  . Input
    //      min_time = 1
    //      max_time = 21
    //      total_size = 100
    //      max_desired_file_size = 30
    //
    //  . Pecentage = 70/100 = 0.3
    //  . Time range between 2 times = (21 - 1) * 0.3 = 6
    //
    //  . Output = [7, 13, 19] in which
    //     7 = 1 (min_time) + 6 (time range)
    //     13 = 7 (previous time) + 6 (time range)
    //     19 = 13 (previous time) + 6 (time range)
    fn compute_split_time(
        chunk_times: Vec<TimestampMinMax>,
        min_time: i64,
        max_time: i64,
        total_size: u64,
        max_desired_file_size: u64,
    ) -> Vec<i64> {
        // Too small to split
        if total_size <= max_desired_file_size {
            return vec![max_time];
        }

        // Same min and max time, nothing to split
        if min_time == max_time {
            return vec![max_time];
        }

        let mut split_times = vec![];
        let percentage = max_desired_file_size as f64 / total_size as f64;
        let mut min = min_time;
        loop {
            let split_time = min + ((max_time - min_time) as f64 * percentage).ceil() as i64;

            if split_time >= max_time {
                break;
            } else if Self::time_range_present(&chunk_times, min, split_time) {
                split_times.push(split_time);
            }
            min = split_time;
        }

        split_times
    }

    // time_range_present returns true if the given time range is included in any of the chunks.
    fn time_range_present(chunk_times: &[TimestampMinMax], min_time: i64, max_time: i64) -> bool {
        chunk_times
            .iter()
            .any(|&chunk| chunk.max >= min_time && chunk.min <= max_time)
    }
}

#[cfg(test)]
mod tests {
    use data_types::TimestampMinMax;

    use crate::components::compact::compact_builder::CompactPlanBuilder;

    #[test]
    fn test_cutoff_bytes() {
        let (small, large) = CompactPlanBuilder::cutoff_bytes(100, 30);
        assert_eq!(small, 30);
        assert_eq!(large, 130);

        let (small, large) = CompactPlanBuilder::cutoff_bytes(100 * 1024 * 1024, 30);
        assert_eq!(small, 30 * 1024 * 1024);
        assert_eq!(large, 130 * 1024 * 1024);

        let (small, large) = CompactPlanBuilder::cutoff_bytes(100, 60);
        assert_eq!(small, 60);
        assert_eq!(large, 160);
    }

    #[test]
    fn test_compute_split_time() {
        let min_time = 1;
        let max_time = 11;
        let total_size = 100;
        let max_desired_file_size = 100;
        let chunk_times = vec![TimestampMinMax {
            min: min_time,
            max: max_time,
        }];

        // no split
        let result = CompactPlanBuilder::compute_split_time(
            chunk_times.clone(),
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], max_time);

        // split 70% and 30%
        let max_desired_file_size = 70;
        let result = CompactPlanBuilder::compute_split_time(
            chunk_times.clone(),
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );
        // only need to store the last split time
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 8); // = 1 (min_time) + 7

        // split 40%, 40%, 20%
        let max_desired_file_size = 40;
        let result = CompactPlanBuilder::compute_split_time(
            chunk_times,
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );
        // store first and second split time
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 5); // = 1 (min_time) + 4
        assert_eq!(result[1], 9); // = 5 (previous split_time) + 4
    }

    #[test]
    fn compute_split_time_when_min_time_equals_max() {
        // Imagine a customer is backfilling a large amount of data and for some reason, all the
        // times on the data are exactly the same. That means the min_time and max_time will be the
        // same, but the total_size will be greater than the desired size.
        // We will not split it becasue the split has to stick to non-overlapped time range

        let min_time = 1;
        let max_time = 1;

        let total_size = 200;
        let max_desired_file_size = 100;
        let chunk_times = vec![TimestampMinMax {
            min: min_time,
            max: max_time,
        }];

        let result = CompactPlanBuilder::compute_split_time(
            chunk_times,
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );

        // must return vector of one containing max_time
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 1);
    }

    #[test]
    fn compute_split_time_please_dont_explode() {
        // degenerated case where the step size is so small that it is < 1 (but > 0). In this case we shall still
        // not loop forever.
        let min_time = 10;
        let max_time = 20;

        let total_size = 600000;
        let max_desired_file_size = 10000;
        let chunk_times = vec![TimestampMinMax {
            min: min_time,
            max: max_time,
        }];

        let result = CompactPlanBuilder::compute_split_time(
            chunk_times,
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );
        assert_eq!(result.len(), 9);
    }

    #[test]
    fn compute_split_time_chunk_gaps() {
        // When the chunks have large gaps, we should not introduce a splits that cause time ranges
        // known to be empty.  Split T2 below should not exist.
        //                   │               │
        //┌────────────────┐                   ┌──────────────┐
        //│    Chunk 1     │ │               │ │   Chunk 2    │
        //└────────────────┘                   └──────────────┘
        //                   │               │
        //                Split T1       Split T2

        // Create a scenario where naive splitting would produce 2 splits (3 chunks) as shown above, but
        // the only chunk data present is in the highest and lowest quarters, similar to what's shown above.
        let min_time = 1;
        let max_time = 100;

        let total_size = 200;
        let max_desired_file_size = total_size / 3;
        let chunk_times = vec![
            TimestampMinMax { min: 1, max: 24 },
            TimestampMinMax { min: 75, max: 100 },
        ];

        let result = CompactPlanBuilder::compute_split_time(
            chunk_times,
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );

        // must return vector of one, containing a Split T1 shown above.
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 34);
    }
}
