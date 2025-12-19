# SMBCollection Fluent API - Adoption Analysis

This document analyzes potential adopters of the new SMBCollection fluent API by examining current usage of traditional SMB APIs in Spotify's codebase.

## Methodology

1. Search for traditional SMB API usage patterns:
   - `sortMergeJoin`
   - `sortMergeTransform`
   - `sortMergeGroupByKey`
   - `sortMergeCoGroup`
   - Multiple `saveAsSortedBucket` calls (multi-output candidates)

2. Group by repository to identify major users

3. Analyze full workflows to identify:
   - Mechanical API migration opportunities (better syntax)
   - Performance optimization opportunities (multi-output, zero-shuffle)

4. Prioritize candidates by potential impact

## Search Results

### Repositories with SMB Usage

From local repository search, identified the following major users:

1. **ubi-pipelines** (User Behavior Insights)
2. **music-core-datasets**
3. **insights-pipelines**
4. **experience-pipelines**
5. **search-post-ranking-data**

## Detailed Analysis

### 1. ubi-pipelines - HIGH PRIORITY CANDIDATE

**Current Usage Pattern:**
- **API**: `sortMergeTransform` with 3-way cogroup
- **Sources**: UBI prod1 impressions + segments + key mapping
- **Outputs**: 2 separate transforms (valid + discarded)
- **Pattern**: Custom wrapper method `sortMergeTransformImpressions`

**Code location:**
`ubi-pipelines/src/main/scala/com/spotify/ubi/impressions/ImpressionPipeline.scala`

**Current Implementation:**
```scala
// Traditional API with callback style
sc.sortMergeTransform(
  classOf[String],
  input, segments, keyMapping,
  TargetParallelism.of(numSmbBuckets)
).to(output)
.withSideInputs(validImpressionPathsSideInput, validParentPathsSideInput)
.via { case (userId, (impressions, segments, keyMapping), ctx, outputCollector) =>
  transformMethod(
    keyMapping,
    ctx(validImpressionPathsSideInput),
    ctx(validParentPathsSideInput),
    impressions,
    segments.map(ImpressionSegmentFact.fromAvro),
    counters
  ).map(outputCollector.accept)  // ❌ Imperative callback
}
```

**Pain Points:**
1. **Imperative callback style** - `outputCollector.accept` instead of functional `map`
2. **Code duplication** - Same transform called twice (valid + discarded outputs)
3. **Potential inefficiency** - Two separate SMB transforms reading same inputs

**Migration to Fluent API:**
```scala
// Single read, shared computation
val base = SMBCollection.cogroup3(
  classOf[String],
  input,
  segments,
  keyMapping,
  TargetParallelism.of(numSmbBuckets)
).withSideInputs(validImpressionPathsSideInput, validParentPathsSideInput)
.flatMap { case ((ctx, (userId, (impressions, segments, keyMapping)))) =>
  transformMethod(
    keyMapping,
    ctx(validImpressionPathsSideInput),
    ctx(validParentPathsSideInput),
    impressions,
    segments.map(ImpressionSegmentFact.fromAvro),
    counters
  )  // ✅ Functional flatMap - no outputCollector!
}

// Multi-output: valid and discarded from same computation
base.map(_.impressionFact).saveAsSortedBucket(validOutput)
base.flatMap(_.invalidImpressions).saveAsSortedBucket(discardedOutput)

sc.run()  // Both outputs share I/O and computation
```

**Benefits:**
- ✅ **Cleaner syntax** - Functional `flatMap` instead of callbacks
- ✅ **Code deduplication** - Single transform, multiple outputs
- ✅ **Potential performance** - If both outputs needed, share I/O (though currently separate pipelines)
- ✅ **Better composability** - Can add more outputs without duplicating reads

**Impact:** HIGH - Used in critical UBI pipelines processing impression events

---

### 2. music-core-datasets

**Current Usage Pattern:**
- **API**: `sortMergeCoGroup` → `flatMap` → SCollection
- **Sources**: artist-user streams + key mapping
- **Purpose**: Decrypt location data and aggregate streams

**Code location:**
`music-core-datasets/src/main/scala/com/spotify/data/music/core/task/ArtistStreamsAndListenersTask.scala`

**Current Implementation:**
```scala
sc.sortMergeCoGroup(
  classOf[String],
  artistUsers,
  keyMappingAll,
  TargetParallelism.max()
).flatMap { case (_, (artistUserStreamsIter, keychainIter)) =>
  // Decrypt and transform
}
```

**Migration Assessment:**
- **Priority**: MEDIUM-LOW
- **Reason**: Already using clean `flatMap` pattern after SMB read
- **Benefit**: Marginal - mostly syntax cleanup (`.toSCollectionAndSeal()` vs direct flatMap)
- **Recommendation**: Migrate only if standardizing on fluent API across codebase

---

## Summary & Recommendations

### High-Priority Migration Candidates

1. **ubi-pipelines** - Multiple SMB transforms with duplicated reads
   - **Estimated Impact**: Cleaner code + potential I/O savings
   - **Migration Effort**: Medium (custom wrapper needs updating)
   - **Stakeholders**: UBI team

### Patterns Found

#### Pattern 1: SMB Transform with outputCollector (High value for fluent API)
```scala
// Traditional - imperative
sc.sortMergeTransform(...).via { case (key, values, outputCollector) =>
  values.foreach(v => outputCollector.accept(transform(v)))
}

// Fluent - functional
SMBCollection.read(...).flatMap(values => values.map(transform))
```

**Repos using this pattern**: ubi-pipelines, insights-pipelines

#### Pattern 2: SMB CoGroup → SCollection (Low value for fluent API)
```scala
// Traditional
sc.sortMergeCoGroup(...).flatMap { case (key, (left, right)) => ... }

// Fluent (minimal improvement)
SMBCollection.cogroup2(...).toSCollectionAndSeal().flatMap { ... }
```

**Repos using this pattern**: music-core-datasets, experience-pipelines

#### Pattern 3: Multiple outputs from same SMB source (Highest value!)
```scala
// Traditional - reads data N times or shuffles N times
sc.sortMergeTransform(...).to(out1).via(transform1)
sc.sortMergeTransform(...).to(out2).via(transform2)

// Fluent - single read, zero shuffles
val base = SMBCollection.read(...).map(expensive)
base.map(_.field1).saveAsSortedBucket(out1)
base.map(_.field2).saveAsSortedBucket(out2)
```

**Potential repos**: Need broader search to find multi-output patterns

### Next Steps

1. **Reach out to MCP code-search** for broader analysis
   - Search for: multiple `sortMergeTransform` calls on same inputs
   - Search for: `sortMergeJoin` followed by multiple `saveAsSortedBucket`
   - Identify repos with multi-output SMB patterns (highest performance impact)

2. **Create migration guide** for common patterns
   - Simple transform migration
   - Multi-output optimization
   - Side inputs usage

3. **Pilot with ubi-pipelines team**
   - High impact, engaged team
   - Can validate fluent API usability
   - Can measure performance improvements

### Estimated Adoption Potential

Based on limited local search:
- **High Priority**: 1-2 repos (ubi-pipelines class)
- **Medium Priority**: 5-10 repos (using SMB transforms)
- **Low Priority**: Many repos (using SMB → SCollection only)

**To get full picture**: Need MCP code-search across all Spotify repos

---

## How to Use This Analysis

For SMBCollection PR reviewers and adopters:

1. **Syntax improvements are universal** - Any `sortMergeTransform` benefits from functional style
2. **Performance wins are selective** - Multi-output patterns see massive gains
3. **Migration is additive** - Can adopt incrementally, no breaking changes to existing code

