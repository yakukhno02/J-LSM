package io.jlsm.compaction;

import io.jlsm.internal.model.ByteKey;
import io.jlsm.sst.TableHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Leveled Compaction Strategy planner organizing SSTables into hierarchical levels (L0..Lmax)
 * with exponential byte capacities, key-range overlap selection, and tombstone safety.
 */
public final class LeveledCompactionPlanner implements CompactionPlanner {

    public static final int DEFAULT_MAX_L0_TABLES = 4;
    public static final long DEFAULT_BASE_LEVEL_SIZE_BYTES = 10L * 1024 * 1024; // 10 MiB
    public static final int DEFAULT_LEVEL_MULTIPLIER = 10;
    public static final int DEFAULT_MAX_LEVELS = 7;

    private final int maxL0Tables;
    private final long baseLevelSizeBytes;
    private final int levelMultiplier;
    private final int maxLevels;
    private final long targetFileSizeBytes;
    private final Map<Long, Integer> tableLevelMap = new ConcurrentHashMap<>();
    private final Function<TableHandle, Integer> customLevelResolver;

    public LeveledCompactionPlanner() {
        this(
                DEFAULT_MAX_L0_TABLES,
                DEFAULT_BASE_LEVEL_SIZE_BYTES,
                DEFAULT_LEVEL_MULTIPLIER,
                DEFAULT_MAX_LEVELS,
                Compactor.DEFAULT_TARGET_FILE_SIZE,
                null
        );
    }

    public LeveledCompactionPlanner(int maxL0Tables, long baseLevelSizeBytes) {
        this(
                maxL0Tables,
                baseLevelSizeBytes,
                DEFAULT_LEVEL_MULTIPLIER,
                DEFAULT_MAX_LEVELS,
                Compactor.DEFAULT_TARGET_FILE_SIZE,
                null
        );
    }

    public LeveledCompactionPlanner(
            int maxL0Tables,
            long baseLevelSizeBytes,
            int levelMultiplier,
            int maxLevels,
            long targetFileSizeBytes
    ) {
        this(maxL0Tables, baseLevelSizeBytes, levelMultiplier, maxLevels, targetFileSizeBytes, null);
    }

    public LeveledCompactionPlanner(
            int maxL0Tables,
            long baseLevelSizeBytes,
            int levelMultiplier,
            int maxLevels,
            long targetFileSizeBytes,
            Function<TableHandle, Integer> customLevelResolver
    ) {
        if (maxL0Tables < 2) {
            throw new IllegalArgumentException("maxL0Tables must be >= 2: " + maxL0Tables);
        }
        if (baseLevelSizeBytes <= 0) {
            throw new IllegalArgumentException("baseLevelSizeBytes must be positive: " + baseLevelSizeBytes);
        }
        if (levelMultiplier < 2) {
            throw new IllegalArgumentException("levelMultiplier must be >= 2: " + levelMultiplier);
        }
        if (maxLevels < 2) {
            throw new IllegalArgumentException("maxLevels must be >= 2: " + maxLevels);
        }
        if (targetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("targetFileSizeBytes must be positive: " + targetFileSizeBytes);
        }
        this.maxL0Tables = maxL0Tables;
        this.baseLevelSizeBytes = baseLevelSizeBytes;
        this.levelMultiplier = levelMultiplier;
        this.maxLevels = maxLevels;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.customLevelResolver = customLevelResolver;
    }

    public int maxL0Tables() {
        return maxL0Tables;
    }

    public long baseLevelSizeBytes() {
        return baseLevelSizeBytes;
    }

    public int levelMultiplier() {
        return levelMultiplier;
    }

    public int maxLevels() {
        return maxLevels;
    }

    public long targetFileSizeBytes() {
        return targetFileSizeBytes;
    }

    public int levelOf(TableHandle handle) {
        Objects.requireNonNull(handle, "handle cannot be null");
        if (customLevelResolver != null) {
            Integer resolved = customLevelResolver.apply(handle);
            if (resolved != null && resolved >= 0 && resolved < maxLevels) {
                return resolved;
            }
        }
        return tableLevelMap.getOrDefault(handle.id(), 0);
    }

    public void assignLevel(long tableId, int level) {
        if (tableId < 0) {
            throw new IllegalArgumentException("tableId cannot be negative: " + tableId);
        }
        if (level < 0 || level >= maxLevels) {
            throw new IllegalArgumentException("level must be in [0, " + (maxLevels - 1) + "]: " + level);
        }
        tableLevelMap.put(tableId, level);
    }

    public void recordCompaction(CompactionPlan plan, List<TableHandle> newTables) {
        Objects.requireNonNull(plan, "plan cannot be null");
        Objects.requireNonNull(newTables, "newTables cannot be null");

        for (TableHandle handle : plan.selectedTables()) {
            tableLevelMap.remove(handle.id());
        }

        int targetLevel = Math.min(plan.tierIndex() + 1, maxLevels - 1);
        for (TableHandle handle : newTables) {
            tableLevelMap.put(handle.id(), targetLevel);
        }
    }

    public long levelCapacityBytes(int level) {
        if (level < 0 || level >= maxLevels) {
            throw new IllegalArgumentException("level must be in [0, " + (maxLevels - 1) + "]: " + level);
        }
        if (level == 0) {
            return (long) maxL0Tables * targetFileSizeBytes;
        }
        long cap = baseLevelSizeBytes;
        for (int i = 1; i < level; i++) {
            if (cap > Long.MAX_VALUE / levelMultiplier) {
                return Long.MAX_VALUE;
            }
            cap *= levelMultiplier;
        }
        return cap;
    }

    public long currentLevelSizeBytes(int level, List<TableHandle> liveTables) {
        Objects.requireNonNull(liveTables, "liveTables cannot be null");
        long total = 0L;
        for (TableHandle handle : liveTables) {
            if (handle != null && levelOf(handle) == level) {
                total += handle.fileSize();
            }
        }
        return total;
    }

    public double levelCompactionScore(int level, List<TableHandle> liveTables) {
        long currentBytes = currentLevelSizeBytes(level, liveTables);
        long capacity = levelCapacityBytes(level);
        return capacity <= 0 ? 0.0 : (double) currentBytes / capacity;
    }

    @Override
    public Optional<CompactionPlan> plan(List<TableHandle> liveTables) {
        checkLiveTables(liveTables);
        if (liveTables.isEmpty()) {
            return Optional.empty();
        }

        List<TableHandle> chronologicallySorted = sortChronologically(liveTables);
        Map<Integer, List<TableHandle>> levels = partitionIntoLevels(chronologicallySorted);

        List<TableHandle> l0Tables = levels.getOrDefault(0, List.of());
        if (l0Tables.size() >= maxL0Tables) {
            return planL0Compaction(l0Tables, levels, chronologicallySorted);
        }

        int bestLevel = -1;
        double bestScore = 1.0;

        for (int i = 1; i < maxLevels - 1; i++) {
            List<TableHandle> levelTables = levels.getOrDefault(i, List.of());
            if (levelTables.isEmpty()) {
                continue;
            }
            long levelBytes = 0L;
            for (TableHandle t : levelTables) {
                levelBytes += t.fileSize();
            }
            long capacity = levelCapacityBytes(i);
            double score = (double) levelBytes / capacity;
            if (score > bestScore) {
                bestScore = score;
                bestLevel = i;
            }
        }

        if (bestLevel != -1) {
            return planHigherLevelCompaction(bestLevel, levels, chronologicallySorted);
        }

        return Optional.empty();
    }

    @Override
    public Optional<CompactionPlan> planFullCompaction(List<TableHandle> liveTables) {
        checkLiveTables(liveTables);
        if (liveTables.isEmpty()) {
            return Optional.empty();
        }
        List<TableHandle> sorted = sortChronologically(liveTables);
        return Optional.of(new CompactionPlan(
                sorted,
                targetFileSizeBytes,
                false,
                true,
                0
        ));
    }

    private static void checkLiveTables(List<TableHandle> liveTables) {
        Objects.requireNonNull(liveTables, "liveTables cannot be null");
        for (TableHandle handle : liveTables) {
            Objects.requireNonNull(handle, "liveTables cannot contain null TableHandle");
        }
    }

    private Optional<CompactionPlan> planL0Compaction(
            List<TableHandle> l0Tables,
            Map<Integer, List<TableHandle>> levels,
            List<TableHandle> allSortedTables
    ) {
        List<TableHandle> selected = new ArrayList<>(l0Tables);
        KeyBounds bounds = findKeyBounds(selected);
        List<TableHandle> l1Tables = levels.getOrDefault(1, List.of());
        selected.addAll(findOverlappingTables(l1Tables, bounds));
        return buildPlan(selected, levels, 1, 0, allSortedTables);
    }

    private Optional<CompactionPlan> planHigherLevelCompaction(
            int sourceLevel,
            Map<Integer, List<TableHandle>> levels,
            List<TableHandle> allSortedTables
    ) {
        List<TableHandle> sourceTables = levels.getOrDefault(sourceLevel, List.of());
        if (sourceTables.isEmpty()) {
            return Optional.empty();
        }

        TableHandle candidate = Collections.max(sourceTables, Comparator.comparingLong(TableHandle::fileSize));
        List<TableHandle> selected = new ArrayList<>();
        selected.add(candidate);

        KeyBounds bounds = findKeyBounds(selected);
        int targetLevel = sourceLevel + 1;
        List<TableHandle> targetTables = levels.getOrDefault(targetLevel, List.of());
        selected.addAll(findOverlappingTables(targetTables, bounds));

        return buildPlan(selected, levels, targetLevel, sourceLevel, allSortedTables);
    }

    private Optional<CompactionPlan> buildPlan(
            List<TableHandle> selected,
            Map<Integer, List<TableHandle>> levels,
            int targetLevel,
            int tierIndex,
            List<TableHandle> allSortedTables
    ) {
        List<TableHandle> ordered = sortChronologically(selected);
        boolean includesBase = isBaseGenerationIncluded(ordered, levels, targetLevel, allSortedTables);
        return Optional.of(new CompactionPlan(
                ordered,
                targetFileSizeBytes,
                !includesBase,
                includesBase,
                tierIndex
        ));
    }

    private Map<Integer, List<TableHandle>> partitionIntoLevels(List<TableHandle> tables) {
        Map<Integer, List<TableHandle>> map = new HashMap<>();
        for (TableHandle handle : tables) {
            int level = Math.clamp(levelOf(handle), 0, maxLevels - 1);
            map.computeIfAbsent(level, k -> new ArrayList<>()).add(handle);
        }
        return map;
    }

    private List<TableHandle> findOverlappingTables(List<TableHandle> tables, KeyBounds bounds) {
        List<TableHandle> result = new ArrayList<>();
        for (TableHandle table : tables) {
            if (overlaps(table, bounds)) {
                result.add(table);
            }
        }
        return result;
    }

    private boolean overlaps(TableHandle table, KeyBounds bounds) {
        if (bounds.minKey() == null && bounds.maxKey() == null) {
            return true;
        }
        Optional<ByteKey> first = table.firstKey();
        Optional<ByteKey> last = table.lastKey();

        if (first.isPresent() && last.isPresent()) {
            ByteKey tMin = first.get();
            ByteKey tMax = last.get();
            return (bounds.minKey() == null || tMax.compareTo(bounds.minKey()) >= 0)
                    && (bounds.maxKey() == null || tMin.compareTo(bounds.maxKey()) <= 0);
        }

        return true;
    }

    private KeyBounds findKeyBounds(List<TableHandle> tables) {
        ByteKey min = null;
        ByteKey max = null;
        for (TableHandle handle : tables) {
            Optional<ByteKey> first = handle.firstKey();
            Optional<ByteKey> last = handle.lastKey();
            if (first.isPresent()) {
                if (min == null || first.get().compareTo(min) < 0) {
                    min = first.get();
                }
            }
            if (last.isPresent()) {
                if (max == null || last.get().compareTo(max) > 0) {
                    max = last.get();
                }
            }
        }
        return new KeyBounds(min, max);
    }

    private boolean isBaseGenerationIncluded(
            List<TableHandle> selected,
            Map<Integer, List<TableHandle>> levels,
            int targetLevel,
            List<TableHandle> allSortedTables
    ) {
        if (allSortedTables.isEmpty() || selected.isEmpty()) {
            return false;
        }
        Set<TableHandle> selectedSet = new HashSet<>(selected);
        if (!selectedSet.contains(allSortedTables.getFirst())) {
            return false;
        }

        for (int l = targetLevel + 1; l < maxLevels; l++) {
            List<TableHandle> deeperTables = levels.getOrDefault(l, List.of());
            if (!deeperTables.isEmpty() && !selectedSet.containsAll(deeperTables)) {
                return false;
            }
        }

        return true;
    }

    private static List<TableHandle> sortChronologically(List<TableHandle> tables) {
        List<TableHandle> copy = new ArrayList<>(tables);
        copy.sort(Comparator
                .comparingLong(TableHandle::minSequence)
                .thenComparingLong(TableHandle::maxSequence)
                .thenComparingLong(TableHandle::id));
        return copy;
    }

    private record KeyBounds(ByteKey minKey, ByteKey maxKey) {}
}
