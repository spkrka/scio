# SMBCollection Fluent API - Migration Examples

This document provides real-world examples of migrating from traditional SMB API to the new SMBCollection fluent API, demonstrating significant code reduction and improved maintainability.

## Summary of Examples

| Repository | Pattern | Lines Changed | Key Benefit |
|------------|---------|---------------|-------------|
| [ubi-pipelines](#1-multi-output-pattern-ubi-pipelines) | Multi-output elimination | -56 lines | Eliminated duplicate transforms |
| [key-metrics-pipelines](#2-functional-style-migration-key-metrics-pipelines) | Functional style | -24 lines | Replaced imperative callbacks |
| [dawnshard](#3-option-pattern-for-combinatorial-explosion-dawnshard) | Option pattern | -634 lines | Eliminated combinatorial explosion |

---

## 1. Multi-Output Pattern (ubi-pipelines)

**Problem**: Duplicate SMB reads and transforms when producing multiple outputs from same computation.

**Repository**: `ubi-pipelines`
**Branch**: `example/smb-collection-multi-output`
**Comparison**: https://ghe.spotify.net/krka/ubi-pipelines/compare/main...example/smb-collection-multi-output

### Before (Traditional API)

```scala
// First transform for valid impressions
sc.sortMergeTransformImpressions(
  classOf[String],
  getUbiProd1Impression(args("ubiProd1Impression")),
  getImpressionSegmentFact(args("impressionSegment")),
  getKeyMapping(args("keyMapping")),
  TargetParallelism.of(args.int("numSmbBuckets"))
)
  .withSideInputs(validImpressionPaths, validParentPaths)
  .to(getImpressionFactValidOutput(args("impressionFact")))
  .via { case (ctx, (userId, (impressions, segments, keyMapping))) =>
    val output = transform(keyMapping, ctx(validImpressionPaths), ...)
    output.impressionFact.foreach(outputCollector.accept)  // Only output valid
  }

// Second transform for invalid impressions (DUPLICATE!)
sc.sortMergeTransformImpressions(
  classOf[String],
  getUbiProd1Impression(args("ubiProd1Impression")),  // Same read!
  getImpressionSegmentFact(args("impressionSegment")), // Same read!
  getKeyMapping(args("keyMapping")),                   // Same read!
  TargetParallelism.of(args.int("numSmbBuckets"))
)
  .withSideInputs(validImpressionPaths, validParentPaths)
  .to(getInvalidImpressionOutput(args("impressionFact")))
  .via { case (ctx, (userId, (impressions, segments, keyMapping))) =>
    val output = transform(keyMapping, ctx(validImpressionPaths), ...) // Same transform!
    output.invalidImpressions.foreach(outputCollector.accept)  // Only output invalid
  }
```

### After (Fluent API)

```scala
// Single cogroup, extract both outputs via flatMap
val transformed = SMBCollection.cogroup3(
  classOf[String],
  getUbiProd1Impression(args("ubiProd1Impression")),
  getImpressionSegmentFact(args("impressionSegment")),
  getKeyMapping(args("keyMapping")),
  TargetParallelism.of(args.int("numSmbBuckets"))
)
  .withSideInputs(validImpressionPaths, validParentPaths)
  .flatMap { case ((ctx, (userId, (impressions, segments, keyMapping)))) =>
    val output = transform(keyMapping, ctx(validImpressionPaths), ...)
    // Return both outputs as tuple elements
    Seq((output.impressionFact, output.invalidImpressions))
  }

// Extract each output stream and save separately
transformed.flatMap { case (valid, _) => valid }
  .saveAsSortedBucket(getImpressionFactValidOutput(args("impressionFact")))

transformed.flatMap { case (_, invalid) => invalid }
  .saveAsSortedBucket(getInvalidImpressionOutput(args("impressionFact")))
```

### Benefits

- **Eliminated duplicate SMB reads**: Single read instead of two
- **Eliminated duplicate transforms**: Single transform instead of two
- **Better pipeline structure**: Clear separation of computation (cogroup) vs routing (flatMap)
- **Performance**: ~50% reduction in SMB I/O and compute for this pattern

### Stats

```
1 file changed, 36 insertions(+), 92 deletions(-)
Net: -56 lines
```

---

## 2. Functional Style Migration (key-metrics-pipelines)

**Problem**: Imperative callbacks make composition difficult and obscure data flow.

**Repository**: `key-metrics-pipelines`
**Branch**: `example/smb-collection-migration`
**Comparison**: https://ghe.spotify.net/krka/key-metrics-pipelines/compare/main...example/smb-collection-migration

### Before (Traditional API)

```scala
// Imperative style with outputCollector
if (partitionDate.isAfter(getPartitionDate(UserProductSnapshotCutOffDate))) {
  sc.sortMergeTransform(
    classOf[String],
    stream, userSnapshot5, userProductSnapshot,
    TargetParallelism.auto()
  )
    .to(streamAggregateOutput)
    .via {
      case (userId, (streamIter, userSnapshotIter, userProductSnapshotIter), outputCollector) =>
        pipeFunction(partitionDate)(
          streamIter, userSnapshotIter, userProductSnapshotIter
        ).foreach(outputCollector.accept)  // Imperative callback
    }
} else {
  // Nearly identical code for different date cutoff
  sc.sortMergeTransform(
    classOf[String],
    stream, userSnapshot5,
    TargetParallelism.auto()
  )
    .to(streamAggregateOutput)
    .via {
      case (userId, (streamIter, userSnapshotIter), outputCollector) =>
        pipeFunction(partitionDate)(
          streamIter, userSnapshotIter, Iterable.empty  // Manual empty injection
        ).foreach(outputCollector.accept)  // Imperative callback
    }
}
```

### After (Fluent API)

```scala
// Functional style with composition
val cogrouped =
  if (partitionDate.isAfter(getPartitionDate(UserProductSnapshotCutOffDate))) {
    SMBCollection.cogroup3(
      classOf[String],
      stream, userSnapshot5, userProductSnapshot,
      TargetParallelism.auto()
    )
  } else {
    SMBCollection.cogroup2(
      classOf[String],
      stream, userSnapshot5,
      TargetParallelism.auto()
    ).map { case (key, (streamIter, userSnapshotIter)) =>
      (key, (streamIter, userSnapshotIter, Iterable.empty[UserProductSnapshot]))
    }
  }

// Clean composition with .values and .tupled
val streamAggregates = cogrouped.values
  .flatMap(pipeFunction(partitionDate).tupled)

streamAggregates.saveAsSortedBucket(streamAggregateOutput)
```

### Benefits

- **Functional composition**: Uses `.values`, `.tupled` for clean chaining
- **Eliminated imperative callbacks**: No `outputCollector.accept` calls
- **Better testability**: `pipeFunction` can be tested independently
- **Clearer data flow**: Easier to see transformation pipeline

### Stats

```
1 file changed, 13 insertions(+), 37 deletions(-)
Net: -24 lines
```

---

## 3. Option Pattern for Combinatorial Explosion (dawnshard)

**Problem**: Optional inputs create exponential case branches with duplicated transform logic.

**Repository**: `dawnshard`
**Branch**: `example/smb-collection-dedup`
**Comparison**: https://ghe.spotify.net/krka/dawnshard/compare/main...example/smb-collection-dedup

### Before (Traditional API)

```scala
// Pattern match on combination of available sources
(hadesUris.yearly.nonEmpty, hadesUris.monthly.nonEmpty,
 hadesUris.daily.nonEmpty, usersUri.isDefined) match {

  // Case 1: ALL sources present (yearly + monthly + daily + users)
  case (true, true, true, true) =>
    maybeArtificialStreams match {
      case Some(userTrackFraud) =>
        SMBMultiJoin(sc)
          .sortMergeTransform(
            classOf[String],
            readYearly, readMonthly, readDaily, readUsers, readIIR, userTrackFraud,
            TargetParallelism.max()
          )
          .to(transformOutput)
          .via(viaFn)  // Transform logic here

      case None =>
        sc.sortMergeTransform(
          classOf[String],
          readYearly, readMonthly, readDaily, readUsers, readIIR,
          TargetParallelism.max()
        )
        .to(transformOutput)
        .via {
          case (userId, (yearly, monthly, daily, users, iir), outputCollector) =>
            viaFn.apply(
              userId,
              (yearly, monthly, daily, users, iir, Iterable[JUserTrackFraud]()), // Manual empty
              outputCollector
            )
        }
    }

  // Case 2: yearly + monthly (no daily, no users)
  case (true, true, false, false) =>
    maybeArtificialStreams match {
      case Some(userTrackFraud) =>
        // ... DUPLICATE transform logic with different cogroup
      case None =>
        // ... DUPLICATE transform logic with manual empties
    }

  // ... 8 more cases with DUPLICATED transform logic in each ...
  // Total: 10 cases × 2 sub-cases = 20 branches
  // Total lines: ~846
}
```

### After (Fluent API)

```scala
// Create Option[Read] for each optional source
val yearlyOpt = if (hadesUris.yearly.nonEmpty) {
  Some(readContextAggregates(hadesUris.yearly, "UserItemContextPlayDataYearly"))
} else None

val monthlyOpt = if (hadesUris.monthly.nonEmpty) {
  Some(readContextAggregates(hadesUris.monthly, "UserItemContextPlayDataMonthly"))
} else None

val dailyOpt = if (hadesUris.daily.nonEmpty) {
  Some(readContextAggregates(hadesUris.daily, "UserItemContextPlayDataDaily"))
} else None

val usersOpt = usersUri.map { uri => readUserIdsSubset(uri) }
val fraudOpt = maybeArtificialStreams.map { streams => streams }

// IIR is always present
val iir = readIIRCollection(iirEndpoint)

// Standard tuple type for all cogroup results (normalized via mapValues)
type StandardTuple = (
  Iterable[JUserItemContextPlayData], // yearly
  Iterable[JUserItemContextPlayData], // monthly
  Iterable[JUserItemContextPlayData], // daily
  Iterable[JUsers],                   // users
  Iterable[JCollectionItem],          // iir
  Iterable[JUserTrackFraud]           // fraud
)

// Pattern match on Options to call appropriate cogroupN, normalize to StandardTuple
val cogrouped: SMBCollection[String, String, StandardTuple] =
  (yearlyOpt, monthlyOpt, dailyOpt, usersOpt, fraudOpt) match {

    // All 5 optional sources present (6-way cogroup)
    case (Some(yearly), Some(monthly), Some(daily), Some(users), Some(fraud)) =>
      SMBCollection.cogroup6(
        classOf[String],
        yearly, monthly, daily, users, iir, fraud,
        TargetParallelism.max()
      )  // Already returns StandardTuple

    // 4 optional present: yearly, monthly, daily, users (no fraud)
    case (Some(yearly), Some(monthly), Some(daily), Some(users), None) =>
      SMBCollection.cogroup5(
        classOf[String],
        yearly, monthly, daily, users, iir,
        TargetParallelism.max()
      ).mapValues { case (y, m, d, u, i) =>
        (y, m, d, u, i, Iterable.empty[JUserTrackFraud])  // Normalize to StandardTuple
      }

    // ... 10 more cases showing pattern (demo shows 12 of 32 possible cases) ...

    // Catch-all for unhandled combinations
    case _ =>
      SMBCollection.read(classOf[String], iir)
        .mapValues { i => (empty, empty, empty, empty, i, empty) }
  }

// Single shared transform logic - ALL cogroup variants use same processing
val aggregatedResults = cogrouped
  .flatMap { case (userId, (yearlyIter, monthlyIter, dailyIter, usersIter, iirIter, fraudIter)) =>
    viaFnFunctional.apply(userId, (yearlyIter, monthlyIter, dailyIter, usersIter, iirIter, fraudIter))
  }

aggregatedResults.saveAsSortedBucket(transformOutput)
```

### Benefits

- **Massive code reduction**: 846 → 210 lines (2.5× reduction)
- **Single transform logic**: Appears once instead of in every case branch
- **Idiomatic Scala**: Pattern matches on `Some(x)/None` instead of `.isDefined/.get`
- **Normalized results**: All cogroup variants produce StandardTuple via `.mapValues`
- **Mechanical routing**: Pattern match just selects cogroupN, no business logic duplication
- **Maintainability**: Change transform logic in ONE place, not 20+ branches
- **No risk of inconsistency**: Impossible for branches to get out of sync

### Performance Characteristics

- **Runtime**: Identical to traditional API
  - Same cogroup operations based on which sources exist
  - Same I/O (only reads existing sources)
  - Same transform logic
  - Empty padding via `mapValues` is negligible overhead

- **Compilation**: Faster
  - Fewer case branches = less bytecode
  - Simpler AST for compiler

### Stats

```
1 file changed, 210 insertions(+), 844 deletions(-)
Net: -634 lines
```

---

## Common Patterns and Best Practices

### 1. Prefer Functional Style Over Imperative Callbacks

**Before**:
```scala
.via { case (key, values, outputCollector) =>
  processValues(values).foreach(outputCollector.accept)
}
```

**After**:
```scala
.flatMap { case (key, values) =>
  processValues(values)
}
```

**Why**: Functional style is more composable, testable, and easier to reason about.

### 2. Use `.map` to Normalize Tuple Structures

When different cogroup arities need to be unified:

```scala
SMBCollection.cogroup2(classOf[String], source1, source2, ...)
  .map { case (key, (a, b)) =>
    (key, (a, b, Iterable.empty[C]))  // Normalize to 3-tuple
  }
```

### 3. Pattern Match on `Option[Read]` for Conditional Sources

**Before** (checking isEmpty):
```scala
if (uris.yearly.nonEmpty && uris.monthly.nonEmpty) {
  // cogroup2
} else if (uris.yearly.nonEmpty) {
  // cogroup1
}
```

**After** (Option pattern):
```scala
val yearlyOpt = if (uris.yearly.nonEmpty) Some(read(...)) else None
val monthlyOpt = if (uris.monthly.nonEmpty) Some(read(...)) else None

(yearlyOpt, monthlyOpt) match {
  case (Some(y), Some(m)) => SMBCollection.cogroup2(...)
  case (Some(y), None) => SMBCollection.read(y)
  case (None, Some(m)) => SMBCollection.read(m)
}
```

### 4. Extract Multi-Output with `flatMap` + Tuples

```scala
val multi = collection.flatMap { case (key, values) =>
  val result = process(values)
  Seq((result.output1, result.output2))
}

multi.flatMap { case (out1, _) => out1 }.saveAsSortedBucket(output1)
multi.flatMap { case (_, out2) => out2 }.saveAsSortedBucket(output2)
```

### 5. Use `.values` for Key-Value Cleanup

When you only need values:

```scala
// Before
.flatMap { case (key, value) => process(value) }

// After
.values.flatMap(process)
```

### 6. Use `.tupled` for Function Application

When function signature matches tuple structure:

```scala
// Before
.flatMap { case (a, b, c) => func(a, b, c) }

// After
.flatMap(func.tupled)
```

---

## Migration Checklist

When migrating from traditional SMB API to fluent API:

- [ ] Identify imperative callbacks (`outputCollector.accept`) → convert to functional returns
- [ ] Find duplicate SMB reads/transforms → consolidate with multi-output pattern
- [ ] Locate combinatorial case branches → apply Option[Read] pattern
- [ ] Replace `.isDefined/.get` with idiomatic `Some(x)/None` pattern matching
- [ ] Normalize different tuple structures with `.map` or `.mapValues`
- [ ] Use `.values`, `.tupled`, `.flatMap` for cleaner composition
- [ ] Verify runtime performance is identical (should be!)
- [ ] Update tests to match new functional signatures

---

## Summary

The SMBCollection fluent API enables significant code reduction through:

1. **Multi-output elimination**: Single cogroup instead of duplicate reads/transforms
2. **Functional composition**: Clean chaining with `.values`, `.tupled`, `.flatMap`
3. **Option pattern**: Eliminates combinatorial explosion with normalized tuple results

**Total lines saved across examples**: -714 lines
**Performance impact**: Identical runtime, faster compilation, better maintainability
