package io.jlsm.compaction;

import io.jlsm.sst.TableHandle;

import java.util.List;
import java.util.Optional;

/**
 * Strategy interface for selecting candidate SSTables for background or forced compaction.
 */
public interface CompactionPlanner {

    /**
     * Evaluates the current list of live SSTables (ordered chronologically from oldest to newest)
     * and returns an optional {@link CompactionPlan} if compaction criteria are met.
     *
     * @param liveTables current live SSTables ordered from oldest to newest
     * @return a {@link CompactionPlan} if a tier or table set qualifies for compaction, or {@link Optional#empty()}
     */
    Optional<CompactionPlan> plan(List<TableHandle> liveTables);

    /**
     * Creates a full major compaction plan covering all non-empty {@code liveTables}, purging tombstones.
     *
     * @param liveTables current live SSTables
     * @return a full {@link CompactionPlan}, or {@link Optional#empty()} if {@code liveTables} is empty
     */
    Optional<CompactionPlan> planFullCompaction(List<TableHandle> liveTables);
}
