// Copyright 2024 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.cas.cfc;

import static build.buildfarm.common.io.Directories.disableAllWriteAccess;
import static build.buildfarm.common.io.Directories.makeWritable;
import static build.buildfarm.common.io.EvenMoreFiles.setReadOnlyPerms;
import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.HOURS;

import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import build.buildfarm.common.io.Utils;
import build.buildfarm.v1test.Digest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.grpc.Deadline;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import lombok.extern.java.Log;
import org.jspecify.annotations.Nullable;

@Log
public class DirectoryEntryCFC extends CASFileCache {
  /** Maximum re-fetch attempts after a source rename race. */
  @VisibleForTesting static final int MAX_REFETCH_ATTEMPTS = 3;

  private static final Counter hardlinkFallbackTotal =
      Counter.build()
          .name("cas_hardlink_fallback_total")
          .help("CAS-directory hardlinks that fell back to byte-copy.")
          .register();
  private static final Counter casDirectoryHardlinksTotal =
      Counter.build()
          .name("cas_directory_hardlinks_total")
          .help(
              "CAS-directory file hardlinks that succeeded (pairs with"
                  + " cas_hardlink_fallback_total).")
          .register();
  private static final Counter hardlinkRefetchAttemptsTotal =
      Counter.build()
          .name("cas_hardlink_race_refetch_attempts_total")
          .help("CAS-directory hardlink re-fetch attempts after a NoSuchFileException rename race.")
          .register();
  private static final Counter hardlinkRaceExhaustedTotal =
      Counter.build()
          .name("cas_hardlink_race_exhausted_total")
          .help("Re-fetch retry budget exhausted; the link failure propagates to the action.")
          .register();
  private static final Counter hardlinkRaceSingleflightFollowersTotal =
      Counter.build()
          .name("cas_hardlink_race_singleflight_followers_total")
          .help("Re-fetch callers that piggybacked on another caller's in-flight re-fetch.")
          .register();
  private static final Counter hardlinkNullSourceEntryTotal =
      Counter.build()
          .name("cas_hardlink_null_source_entry_total")
          .help("linkAndReference found no source Entry (theoretically unreachable; >0 is a bug).")
          .register();
  private static final Gauge inodeMapEntriesGauge =
      Gauge.build()
          .name("cas_hardlink_inode_map_entries")
          .help(
              "Current size of the CAS inode index (source Entries with CAS-directory hardlinks).")
          .register();

  private final Cache<Digest, ListenableFuture<Void>> fetchers = CacheBuilder.newBuilder().build();

  private final CasInodeIndex casInodeIndex = new CasInodeIndex();

  // Compute index size at scrape time to keep it off the hardlink path.
  {
    inodeMapEntriesGauge.setChild(
        new Gauge.Child() {
          @Override
          public double get() {
            return casInodeIndex.size();
          }
        });
  }

  // Coalesces concurrent re-fetches of the same source.
  private final ConcurrentMap<String, ListenableFuture<PathResult>> sourceFileRefetchers =
      new ConcurrentHashMap<>();

  /** Hardlink operation, replaceable by tests. */
  @FunctionalInterface
  interface FileLinker {
    void link(Path source, Path destination) throws IOException;
  }

  @FunctionalInterface
  interface SourceFileKeyReader {
    @Nullable Object fileKey(Path source) throws IOException;
  }

  private volatile FileLinker fileLinker =
      (source, destination) -> Files.createLink(destination, source);

  private volatile SourceFileKeyReader sourceFileKeyReader =
      source -> Utils.toInodeKey(Files.readAttributes(source, BasicFileAttributes.class));

  @VisibleForTesting
  void setFileLinkerForTesting(FileLinker fileLinker) {
    this.fileLinker = fileLinker;
  }

  @VisibleForTesting
  void setSourceFileKeyReaderForTesting(SourceFileKeyReader sourceFileKeyReader) {
    this.sourceFileKeyReader = sourceFileKeyReader;
  }

  @VisibleForTesting
  CasInodeIndex casInodeIndexForTesting() {
    return casInodeIndex;
  }

  private record PendingCasDirectoryHardlink(Entry sourceEntry, Object fileKey) {}

  private record HardlinkSource(Entry sourceEntry, Object fileKey) {}

  public DirectoryEntryCFC(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      int hexBucketLevels,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<String, Entry> storage,
      FixedBufferPool zstdBufferPool,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire,
      @Nullable ContentAddressableStorage delegate,
      boolean delegateSkipLoad,
      InputStreamFactory externalInputStreamFactory) {
    super(
        root,
        maxSizeInBytes,
        maxEntrySizeInBytes,
        hexBucketLevels,
        expireService,
        accessRecorder,
        storage,
        zstdBufferPool,
        onPut,
        onExpire,
        delegate,
        delegateSkipLoad,
        externalInputStreamFactory);
  }

  public DirectoryEntryCFC(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      int hexBucketLevels,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<String, Entry> storage,
      FixedBufferPool zstdBufferPool,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire,
      @Nullable ContentAddressableStorage delegate,
      boolean delegateSkipLoad,
      InputStreamFactory externalInputStreamFactory,
      long lowBytes,
      long wakeBudgetNanos,
      long idleHeartbeatNanos,
      java.time.Clock clock) {
    super(
        root,
        maxSizeInBytes,
        maxEntrySizeInBytes,
        hexBucketLevels,
        expireService,
        accessRecorder,
        storage,
        zstdBufferPool,
        onPut,
        onExpire,
        delegate,
        delegateSkipLoad,
        externalInputStreamFactory,
        lowBytes,
        wakeBudgetNanos,
        idleHeartbeatNanos,
        clock);
  }

  public DirectoryEntryCFC(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      int hexBucketLevels,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<String, Entry> storage,
      FixedBufferPool zstdBufferPool,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire,
      @Nullable ContentAddressableStorage delegate,
      boolean delegateSkipLoad,
      InputStreamFactory externalInputStreamFactory,
      long lowBytes,
      long wakeBudgetNanos,
      long idleHeartbeatNanos,
      java.time.Clock clock,
      int shardCount) {
    super(
        root,
        maxSizeInBytes,
        maxEntrySizeInBytes,
        hexBucketLevels,
        expireService,
        accessRecorder,
        storage,
        zstdBufferPool,
        onPut,
        onExpire,
        delegate,
        delegateSkipLoad,
        externalInputStreamFactory,
        lowBytes,
        wakeBudgetNanos,
        idleHeartbeatNanos,
        clock,
        shardCount);
  }

  private void computeDirectory(
      Path path, Map<Object, Entry> fileKeys, ImmutableList.Builder<Path> invalidDirectories) {
    checkState(standaloneScanComplete, "computeDirectory ran before the standalone scan completed");
    String key = path.getFileName().toString();
    List<PendingCasDirectoryHardlink> pendingHardlinks = new ArrayList<>();
    Set<Object> chargedUnindexedHardlinkInodes = new HashSet<>();
    try {
      AtomicLong blobSizeInBytes = new AtomicLong();
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
              // Directory link counts include child ".." entries, not CAS hardlinks.
              blobSizeInBytes.addAndGet(
                  estimateSizeOnDisk(attrs.size(), blockSize, /* isHardlink= */ false));
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              if (attrs.isRegularFile()) {
                blobSizeInBytes.addAndGet(
                    startupDirectoryFileSize(
                        file, attrs, fileKeys, pendingHardlinks, chargedUnindexedHardlinkInodes));
              }
              return FileVisitResult.CONTINUE;
            }
          });
      Entry e = Entry.orphan(key, blobSizeInBytes.get(), Deadline.after(10, HOURS));
      // Block-align e.size the same way charge() does so the admission check tests the bytes
      // actually reserved below (estimateSizeOnDisk), not the raw aggregate — otherwise a
      // directory could be admitted whose charged cost slightly exceeds the cap.
      long diskSize = estimateSizeOnDisk(e.size, blockSize, /* isHardlink= */ false);
      if (e.size > maxEntrySizeInBytes || e.size == 0 || !tryReserveStartupBytes(key, diskSize)) {
        synchronized (invalidDirectories) {
          invalidDirectories.add(path);
        }
      } else {
        boolean admitted = false;
        try {
          storage.put(key, e);
          // Startup runs before the evictor threads start, so the per-shard LRU lists are not yet
          // under their single-writer contract. linkAtStartup routes this _dir entry to its owning
          // shard and links it under that shard's header lock (serializing concurrent scan
          // threads).
          casShards.linkAtStartup(e, diskSize);
          for (PendingCasDirectoryHardlink pendingHardlink : pendingHardlinks) {
            casInodeIndex.increment(pendingHardlink.sourceEntry(), pendingHardlink.fileKey());
          }
          admitted = true;
        } finally {
          if (!admitted) {
            releaseStartupBytes(key, diskSize);
          }
        }
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "error processing directory " + path.toString(), e);
      synchronized (invalidDirectories) {
        invalidDirectories.add(path);
      }
    }
  }

  @Override
  protected List<Path> computeDirectories(CacheScanResults cacheScanResults)
      throws InterruptedException {
    // create thread pool
    ExecutorService pool = BuildfarmExecutors.getComputeCachePool();

    ImmutableList.Builder<Path> invalidDirectories = new ImmutableList.Builder<>();
    Map<Object, Entry> fileKeys = cacheScanResults.fileKeys();

    for (Path path : cacheScanResults.computeDirs()) {
      pool.execute(() -> computeDirectory(path, fileKeys, invalidDirectories));
    }

    joinThreads(pool, "Populating Directories...");

    return invalidDirectories.build();
  }

  /** Returns the startup charge for a file, avoiding double-charge for indexed hardlinks. */
  private long startupDirectoryFileSize(
      Path file,
      BasicFileAttributes attrs,
      Map<Object, Entry> fileKeys,
      List<PendingCasDirectoryHardlink> pendingHardlinks,
      Set<Object> chargedUnindexedHardlinkInodes) {
    if (readNlink(file) <= 1) {
      return estimateSizeOnDisk(attrs.size(), blockSize, /* isHardlink= */ false);
    }

    Object fileKey = Utils.toInodeKey(attrs);
    if (fileKey == null) {
      return estimateSizeOnDisk(attrs.size(), blockSize, /* isHardlink= */ false);
    }

    Entry sourceEntry = fileKeys == null ? null : fileKeys.get(fileKey);
    if (sourceEntry == null) {
      log.log(
          Level.FINE,
          "computeDirectory: no indexed source entry for hardlinked file "
              + file
              + "; CAS-directory-hardlink pin not reconstructed");
      return chargedUnindexedHardlinkInodes.add(fileKey)
          ? estimateSizeOnDisk(attrs.size(), blockSize, /* isHardlink= */ false)
          : 0;
    }

    pendingHardlinks.add(new PendingCasDirectoryHardlink(sourceEntry, fileKey));
    return estimateSizeOnDisk(attrs.size(), blockSize, /* isHardlink= */ true);
  }

  /** Reads {@code unix:nlink}, defaulting to one so unknown files are fully charged. */
  private static int readNlink(Path path) {
    try {
      Object nlink = Files.getAttribute(path, "unix:nlink");
      if (nlink instanceof Number count) {
        return count.intValue();
      }
    } catch (IOException | UnsupportedOperationException | IllegalArgumentException e) {
      // Use the safe default on filesystems without Unix attributes.
    }
    return 1;
  }

  @Override
  protected void deleteExpiredKey(String expiredKey) throws IOException {
    if (expiredKey.endsWith("_dir")) {
      Path path = getRemovingPath(expiredKey);
      removeCasDirectoryTree(path);
      // accounting for expiration metric?
    } else {
      super.deleteExpiredKey(expiredKey);
    }
  }

  @Override
  protected void removeDirectoryForFetch(Path path) throws IOException {
    removeCasDirectoryTree(path);
  }

  /** Deletes a CAS directory tree and releases its source-entry hardlink pins. */
  private void removeCasDirectoryTree(Path directory) throws IOException {
    Files.walkFileTree(
        directory,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            makeWritable(dir, true, fileStore);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            // Capture the inode before deletion.
            Object fileKey = Utils.toInodeKey(attrs);
            // Windows cannot delete an open file.
            Files.delete(file);
            if (fileKey != null) {
              Entry sourceEntry = casInodeIndex.get(fileKey);
              if (sourceEntry != null && casInodeIndex.decrement(sourceEntry, fileKey) == 0) {
                // The final released pin makes the source evictable.
                casShards.requestEvictionSweep(sourceEntry.key);
              }
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
            if (e != null) {
              throw e;
            }
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private void removeCasDirectoryTreeIfExists(Path path) throws IOException {
    try {
      if (Files.isDirectory(path)) {
        removeCasDirectoryTree(path);
      } else {
        Files.deleteIfExists(path);
      }
    } catch (NoSuchFileException e) {
    }
  }

  /**
   * Materializes {@code src} at {@code dst}, preferring a tracked hardlink and falling back to
   * re-fetch or copy.
   *
   * @return whether {@code dst} shares an already-charged inode
   */
  private boolean linkAndReference(Path dst, Path src, Digest srcDigest, boolean isExecutable)
      throws IOException, InterruptedException {
    try {
      HardlinkSource recordHere = null;
      boolean copyFallback = false;
      boolean materializedAsHardlink;
      try {
        HardlinkSource source = hardlinkSourceOrNull(src);
        if (source == null) {
          copyFallback = true;
          materializedAsHardlink = false;
        } else {
          fileLinker.link(src, dst);
          recordHere = source;
          materializedAsHardlink = true;
        }
      } catch (NoSuchFileException e) {
        if (!linkFailureMayBeMissingSource(e, src)) {
          throw e;
        }
        // Recover if the source was renamed between reference acquisition and link creation.
        log.log(
            Level.WARNING,
            "linkAndReference: source "
                + src.getFileName()
                + " disappeared during link (likely Phase-2.1 evictor rename race); falling back to"
                + " re-fetch",
            e);
        materializedAsHardlink = reFetchAndLink(srcDigest, isExecutable, dst);
      } catch (AccessDeniedException | FileAlreadyExistsException e) {
        // Do not mask permission errors or an unexpectedly existing destination.
        throw e;
      } catch (FileSystemException | UnsupportedOperationException e) {
        // EMLINK, EXDEV, and unsupported hardlinks fall back to a separately charged copy.
        copyFallback = true;
        materializedAsHardlink = false;
      }
      if (copyFallback) {
        copyHardlinkFallback(src, dst);
      }
      // Pin the inode before permission changes can fail and release the transient reference.
      if (recordHere != null) {
        recordCasDirectoryHardlink(recordHere);
      }
      setReadOnlyPerms(dst, isExecutable, fileStore);
      return materializedAsHardlink;
    } finally {
      decrementReference(src.getFileName().toString());
    }
  }

  /** Returns the source entry and inode key needed to track a hardlink, or null to force a copy. */
  private @Nullable HardlinkSource hardlinkSourceOrNull(Path src) throws IOException {
    Entry sourceEntry = storage.get(src.getFileName().toString());
    if (sourceEntry == null) {
      log.log(
          Level.WARNING,
          "linkAndReference: no source entry for "
              + src.getFileName()
              + "; falling back to byte-copy for CAS-directory materialization");
      hardlinkNullSourceEntryTotal.inc();
      return null;
    }
    Object fileKey = Utils.toInodeKey(sourceFileKeyReader.fileKey(src));
    if (fileKey == null) {
      log.log(
          Level.WARNING,
          "linkAndReference: filesystem did not expose an inode key for "
              + src.getFileName()
              + "; falling back to byte-copy for CAS-directory materialization");
      return null;
    }
    return new HardlinkSource(sourceEntry, fileKey);
  }

  private void recordCasDirectoryHardlink(HardlinkSource source) {
    casInodeIndex.increment(source.sourceEntry(), source.fileKey());
    casDirectoryHardlinksTotal.inc();
  }

  private void copyHardlinkFallback(Path src, Path dst) throws IOException {
    Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
    hardlinkFallbackTotal.inc();
  }

  /** Re-fetches and links an evicted source, coalescing concurrent fetches by digest. */
  private boolean reFetchAndLink(Digest srcDigest, boolean isExecutable, Path dst)
      throws IOException, InterruptedException {
    String srcKey = getKey(srcDigest, isExecutable);
    for (int attempt = 1; ; attempt++) {
      hardlinkRefetchAttemptsTotal.inc();
      try {
        return singleflightReFetchAndLink(srcDigest, isExecutable, srcKey, dst);
      } catch (NoSuchFileException e) {
        if (!linkFailureMayBeMissingSource(e, getPath(srcKey))) {
          throw e;
        }
        if (attempt >= MAX_REFETCH_ATTEMPTS) {
          hardlinkRaceExhaustedTotal.inc();
          throw e;
        }
      }
    }
  }

  private boolean singleflightReFetchAndLink(
      Digest srcDigest, boolean isExecutable, String srcKey, Path dst)
      throws IOException, InterruptedException {
    SettableFuture<PathResult> mine = SettableFuture.create();
    ListenableFuture<PathResult> existing = sourceFileRefetchers.putIfAbsent(srcKey, mine);
    PathResult fresh;
    if (existing == null) {
      // put() returns with one reference held for this owner.
      try {
        fresh = put(srcDigest, isExecutable);
        mine.set(fresh);
      } catch (IOException | InterruptedException | RuntimeException e) {
        mine.setException(e);
        throw e;
      } finally {
        sourceFileRefetchers.remove(srcKey, mine);
      }
    } else {
      // Followers must acquire their own reference after the shared fetch completes.
      hardlinkRaceSingleflightFollowersTotal.inc();
      fresh = getInterruptiblyOrIOException(existing);
      if (!referenceIfExists(srcKey)) {
        throw new NoSuchFileException(srcKey);
      }
    }
    try {
      try {
        HardlinkSource source = hardlinkSourceOrNull(fresh.path());
        if (source != null) {
          fileLinker.link(fresh.path(), dst);
          recordCasDirectoryHardlink(source);
          return true;
        }
      } catch (NoSuchFileException e) {
        throw e;
      } catch (AccessDeniedException | FileAlreadyExistsException e) {
        throw e;
      } catch (FileSystemException | UnsupportedOperationException e) {
      }
      copyHardlinkFallback(fresh.path(), dst);
      return false;
    } finally {
      decrementReference(srcKey);
    }
  }

  private static boolean linkFailureMayBeMissingSource(NoSuchFileException e, Path source) {
    return pathStringEquals(e.getFile(), source)
        || pathStringEquals(e.getOtherFile(), source)
        || !Files.exists(source);
  }

  private static boolean pathStringEquals(@Nullable String value, Path path) {
    return value != null && value.equals(path.toString());
  }

  private <T> T getCompleted(ListenableFuture<? extends T> future) {
    checkState(future.isDone());
    try {
      return future.get();
    } catch (ExecutionException e) {
      // must not happen
      throw new UncheckedExecutionException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ListenableFuture<Void> add(
      Path path,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service) {
    String suffix = UUID.randomUUID().toString();
    Path filename = path.getFileName();
    String tmpFilename = filename + ".tmp." + suffix;
    Path tmpPath = path.resolveSibling(tmpFilename);
    AtomicBoolean renamedToFinalPath = new AtomicBoolean();

    ListenableFuture<Long> fetched = fetch(tmpPath, digest, directoriesIndex, service);
    ListenableFuture<Void> limited =
        transformAsync(
            fetched,
            weight -> {
              disableAllWriteAccess(tmpPath, fileStore, /* excludeTopLevel= */ true);
              return immediateFuture(null);
            },
            service);
    ListenableFuture<Void> renamed =
        transformAsync(
            limited,
            result -> {
              Files.move(tmpPath, path);
              renamedToFinalPath.set(true);
              makeWritable(path, /* writable= */ false, fileStore);
              return immediateFuture(result);
            },
            service);
    ListenableFuture<Void> rolled =
        catchingAsync(
            renamed,
            Throwable.class,
            e -> {
              try {
                removeCasDirectoryTreeIfExists(renamedToFinalPath.get() ? path : tmpPath);
              } catch (IOException removeException) {
                e.addSuppressed(removeException);
              }
              return immediateFailedFuture(e);
            },
            service);
    ListenableFuture<Void> stored =
        transformAsync(
            rolled,
            result -> {
              String key = filename.toString();
              long blobSizeInBytes = getCompleted(fetched);
              long diskSize =
                  estimateSizeOnDisk(blobSizeInBytes, blockSize, /* isHardlink= */ false);
              AtomicBoolean charged = new AtomicBoolean();

              try {
                checkState(charge(key, blobSizeInBytes, charged), true);
                Entry e = new Entry(key, blobSizeInBytes, Deadline.after(10, HOURS));
                checkState(safeStorageInsertion(key, e) == null, key);
              } catch (IOException e) {
                if (charged.get()) {
                  casShards.discharge(key, diskSize);
                }
                return immediateFailedFuture(e);
              } catch (RuntimeException e) {
                if (charged.get()) {
                  casShards.discharge(key, diskSize);
                }
                throw e;
              }
              return immediateFuture(result);
            },
            service);
    stored =
        catchingAsync(
            stored,
            Throwable.class,
            e -> {
              if (renamedToFinalPath.get()) {
                try {
                  removeCasDirectoryTreeIfExists(path);
                } catch (IOException removeException) {
                  e.addSuppressed(removeException);
                }
              }
              return immediateFailedFuture(e);
            },
            service);
    // Do not retain a failed fetch and poison later requests for the same digest.
    stored.addListener(() -> fetchers.invalidate(digest), directExecutor());
    return stored;
  }

  private ListenableFuture<Long> fetch(
      Path path,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service) {
    ImmutableList.Builder<ListenableFuture<Path>> putFuturesBuilder = ImmutableList.builder();
    AtomicLong weight = new AtomicLong();
    try {
      long dirOverhead =
          fetchDirectory(
              path,
              digest,
              directoriesIndex,
              (dst, src, srcDigest, size, isExecutable) -> {
                boolean hardlinked = linkAndReference(dst, src, srcDigest, isExecutable);
                // Only copy fallback creates new file blocks.
                weight.addAndGet(estimateSizeOnDisk(size, blockSize, /* isHardlink= */ hardlinked));
              },
              putFuturesBuilder,
              service);
      weight.addAndGet(dirOverhead);
    } catch (Exception e) {
      ImmutableList<ListenableFuture<Path>> putFutures = putFuturesBuilder.build();
      return failAfterPutFutures(path, digest, putFutures, e, service);
    }
    ImmutableList<ListenableFuture<Path>> putFutures = putFuturesBuilder.build();

    // is there a better wrapper for this?
    // should probably destroy the directory on failure
    return transformAsync(
        successfulAsList(putFutures),
        paths -> {
          ImmutableList<Throwable> failures = collectPutFailures(putFutures, paths);
          if (!failures.isEmpty()) {
            return immediateFailedFuture(new PutDirectoryException(path, digest, failures));
          }
          return immediateFuture(weight.get());
        },
        service);
  }

  private ListenableFuture<Long> failAfterPutFutures(
      Path path,
      Digest digest,
      ImmutableList<ListenableFuture<Path>> putFutures,
      Throwable failure,
      ExecutorService service) {
    if (putFutures.isEmpty()) {
      return immediateFailedFuture(failure);
    }
    return transformAsync(
        successfulAsList(putFutures),
        paths -> {
          ImmutableList.Builder<Throwable> failures = ImmutableList.builder();
          failures.add(failure);
          failures.addAll(collectPutFailures(putFutures, paths));
          return immediateFailedFuture(new PutDirectoryException(path, digest, failures.build()));
        },
        service);
  }

  private static ImmutableList<Throwable> collectPutFailures(
      ImmutableList<ListenableFuture<Path>> putFutures, List<Path> paths) {
    ImmutableList.Builder<Throwable> failures = ImmutableList.builder();
    for (int i = 0; i < paths.size(); i++) {
      Path putPath = paths.get(i);
      if (putPath == null) {
        try {
          putFutures.get(i).get();
        } catch (ExecutionException e) {
          failures.add(e.getCause());
        } catch (Throwable t) {
          failures.add(t);
        }
      }
    }
    return failures.build();
  }

  public ListenableFuture<PathResult> putDirectory(
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service) {
    Path path = getDirectoryPath(digest);
    String key = path.getFileName().toString();
    try {
      if (referenceIfExists(key)) {
        return immediateFuture(new PathResult(path, false));
      }
    } catch (IOException e) {
      return immediateFailedFuture(e);
    }
    // we must acquire a reference to this entry before delivering it to the client
    // unfortunately that means that it will be placed onto the LRU queue briefly
    // we could probably retry the whole procedure in that case, but maybe there is
    // simply too much contention at that point
    //
    // to correct this, we could find a way to identify the fetching thread and perform
    // the dereference only after all of the callbacks have been satisfied. Very funky
    // future stuff would need to be in place.
    //
    // we can play games with the immediacy of the callback though
    ListenableFuture<Void> fetch;
    // true if we were the creator of the fetch
    AtomicBoolean owner = new AtomicBoolean();
    try {
      fetch =
          fetchers.get(
              digest,
              () -> {
                owner.set(true);
                return add(path, digest, directoriesIndex, service);
              });
    } catch (ExecutionException e) {
      return immediateFailedFuture(e);
    }
    // we wil need to increment the reference count, as we did not call the future creator
    if (!owner.get()) {
      fetch =
          transformAsync(
              fetch,
              result -> {
                // completely unacceptable if it does not exist or is not available for reference
                // here
                try {
                  checkState(referenceIfExists(key));
                } catch (IOException e) {
                  return immediateFailedFuture(e);
                }
                return immediateFuture(result);
              },
              directExecutor());
    }
    return transform(fetch, result -> new PathResult(path, true), directExecutor());
  }

  @Override
  public void decrementReferences(
      Iterable<String> inputFiles,
      Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories,
      DigestFunction.Value digestFunction)
      throws IOException {
    Iterable<String> directoryDigests =
        Iterables.transform(
            inputDirectories,
            digest -> getDirectoryKey(DigestUtil.fromDigest(digest, digestFunction)));
    decrementInputReferences(Iterables.concat(inputFiles, directoryDigests));
  }

  @Override
  protected List<ListenableFuture<Void>> unlinkAndExpireDirectories(
      Entry entry, ExecutorService service) {
    // The evictor has already detached this entry from the LRU before calling. No
    // directory-table state to drain (DirectoryEntryCFC uses inline trees).
    if (entry.refCount() != 0) {
      log.log(Level.SEVERE, "removed referenced entry " + entry.key);
    }
    return ImmutableList.of(immediateFuture(null));
  }
}
