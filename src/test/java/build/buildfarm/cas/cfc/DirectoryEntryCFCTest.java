// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache.Entry;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class DirectoryEntryCFCTest {
  protected static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  protected CASFileCache fileCache;
  private final Path root;
  private Map<Digest, ByteString> blobs;
  private ExecutorService putService;

  @Mock private Consumer<Digest> onPut;

  @Mock private Consumer<Iterable<Digest>> onExpire;

  @Mock private ContentAddressableStorage delegate;

  private ExecutorService expireService;

  protected ConcurrentMap<String, Entry> storage;

  protected DirectoryEntryCFCTest(Path fileSystemRoot) {
    this.root = fileSystemRoot.resolve("cache");
  }

  /** Whether this filesystem reports directory sizes needed for startup admission. */
  protected boolean startupAdmitsExistingDirectoryTrees() {
    return false;
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    when(delegate.getWrite(
            any(Compressor.Value.class),
            any(Digest.class),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(new NullWrite());
    when(delegate.newInput(any(Compressor.Value.class), any(Digest.class), any(Long.class)))
        .thenThrow(new NoSuchFileException("null sink delegate"));
    doAnswer(
            (Answer<Iterable<Digest>>)
                invocation -> {
                  return (Iterable<Digest>) invocation.getArguments()[0];
                })
        .when(delegate)
        .findMissingBlobs(any(Iterable.class), any(DigestFunction.Value.class));
    blobs = Maps.newHashMap();
    putService = newSingleThreadExecutor();
    storage = Maps.newConcurrentMap();
    expireService = newSingleThreadExecutor();
    fileCache =
        new DirectoryEntryCFC(
            root,
            /* maxSizeInBytes= */ 1024 * 1024,
            /* maxEntrySizeInBytes= */ 1024 * 1024,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            storage,
            /* zstdBufferPool= */ null,
            onPut,
            onExpire,
            delegate,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> {
              ByteString content = blobs.get(digest);
              if (content == null) {
                return fileCache.newTransparentInput(compressor, digest, offset);
              }
              checkArgument(compressor == Compressor.Value.IDENTITY);
              return content.substring((int) offset).newInput();
            });
    fileCache.initializeRootDirectory();
    // Force a known block size so tests are hermetic and don't depend on the host filesystem.
    fileCache.setBlockSizeForTesting(4096);
    // Phase 2.1: start the evictor so eviction-triggering operations work in the fixture.
    fileCache.evictorForTesting().start();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    FileStore fileStore = Files.getFileStore(root);
    fileCache.evictorForTesting().stop();
    if (!shutdownAndAwaitTermination(putService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down put service");
    }
    if (!shutdownAndAwaitTermination(expireService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down expire service");
    }
    Directories.remove(root, fileStore);
  }

  @Test
  public void estimateDirectorySizeOnDisk_emptyDirectory_returnsOneBlock() {
    Directory emptyDir = Directory.getDefaultInstance();
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(emptyDir, 4096)).isEqualTo(4096);
  }

  @Test
  public void estimateDirectorySizeOnDisk_fewEntries_returnsOneBlock() {
    Directory dir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file1")
                    .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("subdir")
                    .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
                    .build())
            .build();
    // 2 entries * 32 bytes = 64 bytes, fits in one 4096-byte block
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir, 4096)).isEqualTo(4096);
  }

  @Test
  public void estimateDirectorySizeOnDisk_manyEntries_scalesWithEntryCount() {
    Directory.Builder dir = Directory.newBuilder();
    for (int i = 0; i < 200; i++) {
      dir.addFiles(
          FileNode.newBuilder()
              .setName("file" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    // 200 entries * 32 bytes = 6400 bytes, needs 2 blocks of 4096
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir.build(), 4096)).isEqualTo(8192);
  }

  @Test
  public void estimateDirectorySizeOnDisk_countsAllEntryTypes() {
    Directory.Builder dir = Directory.newBuilder();
    for (int i = 0; i < 80; i++) {
      dir.addFiles(
          FileNode.newBuilder()
              .setName("file" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    for (int i = 0; i < 60; i++) {
      dir.addDirectories(
          DirectoryNode.newBuilder()
              .setName("dir" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    for (int i = 0; i < 60; i++) {
      dir.addSymlinks(SymlinkNode.newBuilder().setName("link" + i).setTarget("target").build());
    }
    // 200 total entries * 32 bytes = 6400 bytes, needs 2 blocks
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir.build(), 4096)).isEqualTo(8192);
  }

  // -- estimateSizeOnDisk tests --

  @Test
  public void estimateSizeOnDisk_zeroSize_returnsZero() {
    assertThat(CASFileCache.estimateSizeOnDisk(0, 4096, /* isHardlink= */ false)).isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_oneByte_returnsOneBlock() {
    assertThat(CASFileCache.estimateSizeOnDisk(1, 4096, /* isHardlink= */ false)).isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_justUnderOneBlock_returnsOneBlock() {
    assertThat(CASFileCache.estimateSizeOnDisk(4095, 4096, /* isHardlink= */ false))
        .isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_exactlyOneBlock_returnsOneBlock() {
    // Idempotent: an already-aligned value should not round up to the next block.
    assertThat(CASFileCache.estimateSizeOnDisk(4096, 4096, /* isHardlink= */ false))
        .isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_oneByteOverBlock_returnsTwoBlocks() {
    assertThat(CASFileCache.estimateSizeOnDisk(4097, 4096, /* isHardlink= */ false))
        .isEqualTo(8192);
  }

  @Test
  public void estimateSizeOnDisk_negativeSize_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(-1, 4096, /* isHardlink= */ false));
  }

  @Test
  public void estimateSizeOnDisk_differentBlockSizes() {
    assertThat(CASFileCache.estimateSizeOnDisk(100, 512, /* isHardlink= */ false)).isEqualTo(512);
    assertThat(CASFileCache.estimateSizeOnDisk(100, 1, /* isHardlink= */ false)).isEqualTo(100);
  }

  @Test
  public void estimateSizeOnDisk_invalidBlockSize_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(100, 0, /* isHardlink= */ false));
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(100, -1, /* isHardlink= */ false));
  }

  @Test
  public void estimateSizeOnDisk_isIdempotent() {
    // Applying estimateSizeOnDisk twice should give the same result as applying it once.
    // This matters because directory Entry.size values are pre-aligned, and discharge()
    // applies estimateSizeOnDisk again.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 8192, 10000};
    long[] blockSizes = {512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        long once = CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false);
        long twice = CASFileCache.estimateSizeOnDisk(once, blockSize, /* isHardlink= */ false);
        assertThat(twice).isEqualTo(once);
      }
    }
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_returnsZero() {
    // Hardlinks reuse an existing inode's blocks, so the physical cost is ~0 regardless of
    // logical size or block size.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 1L << 20};
    long[] blockSizes = {1, 512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        assertThat(CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ true))
            .isEqualTo(0);
      }
    }
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_zeroSize_returnsZero() {
    // Zero-size boundary with the hardlink branch still returns 0.
    assertThat(CASFileCache.estimateSizeOnDisk(0, 4096, /* isHardlink= */ true)).isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_largeSize_returnsZero() {
    // Integer.MAX_VALUE-scale sizes with the hardlink branch still return 0 and do not overflow.
    assertThat(CASFileCache.estimateSizeOnDisk(Integer.MAX_VALUE, 4096, /* isHardlink= */ true))
        .isEqualTo(0);
    assertThat(
            CASFileCache.estimateSizeOnDisk(
                (long) Integer.MAX_VALUE + 1, 4096, /* isHardlink= */ true))
        .isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_isIdempotent() {
    // Applying estimateSizeOnDisk twice with isHardlink=true still yields 0, mirroring the
    // existing estimateSizeOnDisk_isIdempotent test.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 8192, 10000};
    long[] blockSizes = {512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        long once = CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ true);
        long twice = CASFileCache.estimateSizeOnDisk(once, blockSize, /* isHardlink= */ true);
        assertThat(twice).isEqualTo(once);
      }
    }
  }

  @Test
  public void directoryChargeDischargeSizeReturnsToZero() throws IOException, InterruptedException {
    // Put a directory, verify it charges sizeInBytes, then evict it and verify size returns to
    // zero.
    ByteString file = ByteString.copyFromUtf8("Hello Directory");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, directoriesIndex, putService));

    long sizeAfterPut = fileCache.size();
    assertThat(sizeAfterPut).isGreaterThan(0);

    // Dereference the directory entry so it becomes evictable.
    fileCache.decrementReferences(
        ImmutableList.of(),
        ImmutableList.of(DigestUtil.toDigest(dirDigest)),
        DIGEST_UTIL.getDigestFunction());

    // Put a large blob that forces eviction of the directory entry.
    byte[] bigData = new byte[(int) (fileCache.maxSize() - 100)];
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigBlob);
    blobs.put(bigDigest, bigBlob);
    fileCache.put(bigDigest, false);

    // After eviction and new charge, size should exactly equal the big blob's on-disk size —
    // no drift from asymmetric charge/discharge of the directory entry.
    assertThat(fileCache.size())
        .isEqualTo(CASFileCache.estimateSizeOnDisk(bigData.length, 4096, /* isHardlink= */ false));
  }

  @Test
  public void putDirectoryIncludesDirectoryOverheadInSize()
      throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("Peanut Butter");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory subDirectory = Directory.getDefaultInstance();
    Digest subdirDigest = DIGEST_UTIL.compute(subDirectory);
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("subdir")
                    .setDigest(DigestUtil.toDigest(subdirDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(dirDigest), directory,
            DigestUtil.toDigest(subdirDigest), subDirectory);

    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, directoriesIndex, putService));

    // Content is charged once; the directory contributes only table overhead.
    long standalone = CASFileCache.estimateSizeOnDisk(file.size(), 4096, /* isHardlink= */ false);
    long directoryOverhead =
        CASFileCache.estimateDirectorySizeOnDisk(directory, 4096)
            + CASFileCache.estimateDirectorySizeOnDisk(subDirectory, 4096);
    assertThat(fileCache.size()).isEqualTo(standalone + directoryOverhead);
  }

  /** Builds a single-file directory fixture. */
  private PutDirectoryFixture putSingleFileDirectory(ByteString file)
      throws IOException, InterruptedException {
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);
    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, directoriesIndex, putService));
    return new PutDirectoryFixture(fileDigest, dirDigest);
  }

  private record PutDirectoryFixture(Digest fileDigest, Digest dirDigest) {}

  private Object fileKeyOf(Path path) throws IOException {
    return Files.readAttributes(path, BasicFileAttributes.class).fileKey();
  }

  @Test
  public void linkAndReference_succeeds_hardlinkSharesFileKey()
      throws IOException, InterruptedException {
    PutDirectoryFixture f = putSingleFileDirectory(ByteString.copyFromUtf8("hardlink me"));

    Path standalone = fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false));
    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");

    assertThat(fileKeyOf(inDirectory)).isEqualTo(fileKeyOf(standalone));
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(1);
  }

  @Test
  public void linkAndReference_oneFileTwoDirectories_collapsesToSingleInodeWithCountTwo()
      throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("shared toolchain file");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory d1 =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("alpha")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Directory d2 =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("beta")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest d1Digest = DIGEST_UTIL.compute(d1);
    Digest d2Digest = DIGEST_UTIL.compute(d2);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(
            DigestUtil.toDigest(d1Digest), d1,
            DigestUtil.toDigest(d2Digest), d2);

    getInterruptiblyOrIOException(fileCache.putDirectory(d1Digest, index, putService));
    getInterruptiblyOrIOException(fileCache.putDirectory(d2Digest, index, putService));

    Entry source = storage.get(CASFileCache.getKey(fileDigest, false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(2);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(1);
  }

  @Test
  public void linkAndReference_fileSystemException_fallsBackToCopy()
      throws IOException, InterruptedException {
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              throw new FileSystemException(destination.toString(), null, "simulated ENOLINK");
            });

    ByteString file = ByteString.copyFromUtf8("copy fallback content");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path standalone = fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false));
    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");

    assertThat(fileKeyOf(inDirectory)).isNotEqualTo(fileKeyOf(standalone));
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void linkAndReference_unsupportedOperation_fallsBackToCopy()
      throws IOException, InterruptedException {
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              throw new UnsupportedOperationException("hardlinks unavailable");
            });

    ByteString file = ByteString.copyFromUtf8("unsupported hardlink content");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void linkAndReference_nullSourceFileKey_fallsBackToCopy()
      throws IOException, InterruptedException {
    ((DirectoryEntryCFC) fileCache).setSourceFileKeyReaderForTesting(source -> null);

    ByteString file = ByteString.copyFromUtf8("null file key fallback");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path standalone = fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false));
    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");

    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    assertThat(fileKeyOf(inDirectory)).isNotEqualTo(fileKeyOf(standalone));
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
  }

  @Test
  public void linkAndReference_noSuchFileException_fallsBackToReFetch()
      throws IOException, InterruptedException {
    AtomicInteger linkCalls = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              if (linkCalls.getAndIncrement() == 0) {
                throw new NoSuchFileException(source.toString());
              }
              Files.createLink(destination, source);
            });

    ByteString file = ByteString.copyFromUtf8("refetched content");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    assertThat(linkCalls.get()).isAtLeast(2);
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(fileKeyOf(inDirectory))
        .isEqualTo(fileKeyOf(fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false))));
  }

  @Test
  public void linkAndReference_noSuchFileForDestinationDoesNotRefetch()
      throws IOException, InterruptedException {
    AtomicInteger linkCalls = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              linkCalls.incrementAndGet();
              throw new NoSuchFileException(destination.toString());
            });

    assertThrows(
        Exception.class,
        () -> putSingleFileDirectory(ByteString.copyFromUtf8("missing destination")));

    assertThat(linkCalls.get()).isEqualTo(1);
  }

  @Test
  public void linkAndReference_reFetchFileSystemException_fallsBackToCopy()
      throws IOException, InterruptedException {
    AtomicInteger linkCalls = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              if (linkCalls.getAndIncrement() == 0) {
                throw new NoSuchFileException(source.toString());
              }
              throw new FileSystemException(destination.toString(), null, "simulated EMLINK");
            });

    ByteString file = ByteString.copyFromUtf8("refetch copy fallback");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(linkCalls.get()).isAtLeast(2);
  }

  @Test
  public void linkAndReference_reFetchNullSourceFileKey_fallsBackToCopy()
      throws IOException, InterruptedException {
    AtomicInteger linkCalls = new AtomicInteger();
    AtomicInteger fileKeyReads = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              linkCalls.incrementAndGet();
              throw new NoSuchFileException(source.toString());
            });
    ((DirectoryEntryCFC) fileCache)
        .setSourceFileKeyReaderForTesting(
            source -> fileKeyReads.getAndIncrement() == 0 ? new Object() : null);

    ByteString file = ByteString.copyFromUtf8("refetch null file key fallback");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path standalone = fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false));
    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");

    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    assertThat(fileKeyOf(inDirectory)).isNotEqualTo(fileKeyOf(standalone));
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
    assertThat(linkCalls.get()).isEqualTo(1);
    assertThat(fileKeyReads.get()).isAtLeast(2);
  }

  @Test
  public void linkAndReference_failureDecrementsReferenceExactlyOnce()
      throws IOException, InterruptedException {
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              throw new FileSystemException(destination.toString(), null, "simulated ENOLINK");
            });

    PutDirectoryFixture f = putSingleFileDirectory(ByteString.copyFromUtf8("release once"));

    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.refCount()).isEqualTo(0);
    assertThat(source.isEvictable()).isTrue();
  }

  @Test
  public void putDirectory_duplicateChildDirectoryRejectedBeforeMaterialization() throws Exception {
    ByteString file = ByteString.copyFromUtf8("duplicate child hardlink");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory child =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest childDigest = DIGEST_UTIL.compute(child);
    Directory root =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("dup")
                    .setDigest(DigestUtil.toDigest(childDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("dup")
                    .setDigest(DigestUtil.toDigest(childDigest))
                    .build())
            .build();
    Digest rootDigest = DIGEST_UTIL.compute(root);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDigest), root,
            DigestUtil.toDigest(childDigest), child);

    assertThrows(
        Exception.class,
        () -> getInterruptiblyOrIOException(fileCache.putDirectory(rootDigest, index, putService)));

    Entry source = storage.get(CASFileCache.getKey(fileDigest, false));
    if (source != null) {
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    }
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
    assertNoTempDirectorySiblings(rootDigest);
  }

  @Test
  public void putDirectory_failureCleansRecordedHardlinkPins()
      throws IOException, InterruptedException {
    ByteString first = ByteString.copyFromUtf8("first linked before failure");
    ByteString second = ByteString.copyFromUtf8("second fails");
    Digest firstDigest = DIGEST_UTIL.compute(first);
    Digest secondDigest = DIGEST_UTIL.compute(second);
    blobs.put(firstDigest, first);
    blobs.put(secondDigest, second);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("first")
                    .setDigest(DigestUtil.toDigest(firstDigest))
                    .build())
            .addFiles(
                FileNode.newBuilder()
                    .setName("second")
                    .setDigest(DigestUtil.toDigest(secondDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              if (destination.getFileName().toString().equals("second")) {
                throw new AccessDeniedException(destination.toString());
              }
              Files.createLink(destination, source);
            });

    assertThrows(
        Exception.class,
        () -> getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, index, putService)));

    Entry firstSource = storage.get(CASFileCache.getKey(firstDigest, false));
    Entry secondSource = storage.get(CASFileCache.getKey(secondDigest, false));
    assertThat(firstSource.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(secondSource.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);

    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting((source, destination) -> Files.createLink(destination, source));
    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, index, putService));

    assertThat(firstSource.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(secondSource.casDirectoryHardlinkCount()).isEqualTo(1);
  }

  @Test
  public void putDirectory_synchronousTreeFailureWaitsForMaterializersBeforeCleanup()
      throws Exception {
    ByteString file = ByteString.copyFromUtf8("linked before missing child");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);
    Digest missingChildDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("missing child"));

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("child")
                    .setDigest(DigestUtil.toDigest(missingChildDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    CountDownLatch linkStarted = new CountDownLatch(1);
    CountDownLatch releaseLink = new CountDownLatch(1);
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              linkStarted.countDown();
              try {
                releaseLink.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
              }
              Files.createLink(destination, source);
            });

    ListenableFuture<CASFileCache.PathResult> future =
        fileCache.putDirectory(dirDigest, index, putService);

    assertThat(linkStarted.await(5, SECONDS)).isTrue();
    assertThat(future.isDone()).isFalse();

    releaseLink.countDown();
    assertThrows(Exception.class, () -> getInterruptiblyOrIOException(future));

    Entry source = storage.get(CASFileCache.getKey(fileDigest, false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
    assertNoTempDirectorySiblings(dirDigest);
  }

  private void assertNoTempDirectorySiblings(Digest dirDigest) throws IOException {
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dirPath.getParent(), dirPath.getFileName() + ".tmp.*")) {
      assertThat(stream).isEmpty();
    }
  }

  @Test
  public void evictor_fileWithCasDirectoryHardlinks_evictableOnlyAfterDirectoryEvicts()
      throws IOException, InterruptedException {
    // The final hardlink release must wake eviction so the following charge can complete.
    PutDirectoryFixture f = putSingleFileDirectory(ByteString.copyFromUtf8("pin me"));
    String fileKey = CASFileCache.getKey(f.fileDigest(), false);
    String dirKey = fileCache.getDirectoryKey(f.dirDigest());

    fileCache.decrementReferences(
        ImmutableList.of(),
        ImmutableList.of(DigestUtil.toDigest(f.dirDigest())),
        DIGEST_UTIL.getDigestFunction());
    assertThat(storage.get(fileKey).casDirectoryHardlinkCount()).isEqualTo(1);

    // Requiring the whole cache makes both directory and source eviction deterministic.
    byte[] big = new byte[(int) (fileCache.maxSize() - 100)];
    Digest bigDigest = DIGEST_UTIL.compute(ByteString.copyFrom(big));
    blobs.put(bigDigest, ByteString.copyFrom(big));
    fileCache.put(bigDigest, false);

    assertThat(storage.get(dirKey)).isNull();
    assertThat(storage.get(fileKey)).isNull();
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
  }

  @Test
  public void putDirectory_initialFailureDoesNotPoisonFetchers()
      throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("eventually available");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    assertThrows(
        Exception.class,
        () -> getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, index, putService)));

    blobs.put(fileDigest, file);
    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, index, putService));

    Path inDirectory = fileCache.getDirectoryPath(dirDigest).resolve("file");
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
  }

  @Test
  public void startupScan_existingDirectoryTrees_populatesInodeMap() throws Exception {
    byte[] content = "startup hardlinked file".getBytes(StandardCharsets.UTF_8);
    Digest fileDigest = DIGEST_UTIL.compute(ByteString.copyFrom(content));
    String fileKey = CASFileCache.getKey(fileDigest, false);
    Path standalone = fileCache.getPath(fileKey);
    Files.write(standalone, content);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    Files.createDirectories(dirPath);
    Files.createLink(dirPath.resolve("file"), standalone);

    getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

    Entry source = storage.get(fileKey);
    assertThat(source).isNotNull();
    Entry directoryEntry = storage.get(fileCache.getDirectoryKey(dirDigest));
    if (startupAdmitsExistingDirectoryTrees()) {
      assertThat(directoryEntry).isNotNull();
    } else if (directoryEntry == null) {
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
      assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
      return;
    }
    Object standaloneFileKey = fileKeyOf(standalone);
    Object directoryFileKey = fileKeyOf(dirPath.resolve("file"));
    if (standaloneFileKey != null && standaloneFileKey.equals(directoryFileKey)) {
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(1);
      assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(1);
    } else {
      // Providers without stable file keys cannot reconstruct the pin.
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
      assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
      assertThat(directoryEntry.size)
          .isAtLeast(
              CASFileCache.estimateSizeOnDisk(content.length, 4096, /* isHardlink= */ false));
    }
    assertThat(source.refCount()).isEqualTo(0);
  }

  @Test
  public void startupScan_snapshotFileWithDirectoryTrees_populatesInodeMap() throws Exception {
    byte[] content = "startup snapshot hardlinked file".getBytes(StandardCharsets.UTF_8);
    Digest fileDigest = DIGEST_UTIL.compute(ByteString.copyFrom(content));
    String fileKey = CASFileCache.getKey(fileDigest, false);
    Path standalone = fileCache.getPath(fileKey);
    Files.write(standalone, content);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    String dirKey = fileCache.getDirectoryKey(dirDigest);
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    Files.createDirectories(dirPath);
    Files.createLink(dirPath.resolve("file"), standalone);

    String snapshot = fileKey + "," + content.length + "\n" + dirKey + ",1\n";
    Files.write(
        root.resolve(LruSnapshotFiles.name(1, 0)), snapshot.getBytes(StandardCharsets.UTF_8));

    getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

    Entry source = storage.get(fileKey);
    assertThat(source).isNotNull();
    Entry directoryEntry = storage.get(dirKey);
    if (startupAdmitsExistingDirectoryTrees()) {
      assertThat(directoryEntry).isNotNull();
    } else if (directoryEntry == null) {
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
      return;
    }
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(1);
  }

  @Test
  public void put_thenReference_doesNotTouchCasDirectoryHardlinkCount() throws Exception {
    ByteString blob = ByteString.copyFromUtf8("plain reference");
    Digest digest = DIGEST_UTIL.compute(blob);
    blobs.put(digest, blob);
    fileCache.put(digest, false); // creates and references the entry

    String key = CASFileCache.getKey(digest, false);
    Entry entry = storage.get(key);
    assertThat(entry.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(entry.refCount()).isAtLeast(1);

    assertThat(fileCache.referenceIfExists(key)).isTrue();
    assertThat(entry.casDirectoryHardlinkCount()).isEqualTo(0);

    fileCache.decrementReference(key);
    fileCache.decrementReference(key);
    assertThat(entry.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void linkAndReference_reFetchExhausted_propagatesAfterMaxAttempts() {
    AtomicInteger linkCalls = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              linkCalls.incrementAndGet();
              throw new NoSuchFileException(source.toString());
            });

    assertThrows(
        Exception.class,
        () -> putSingleFileDirectory(ByteString.copyFromUtf8("always races away")));

    assertThat(linkCalls.get()).isEqualTo(1 + DirectoryEntryCFC.MAX_REFETCH_ATTEMPTS);
  }

  @Test
  public void startupScan_directoryHardlinkWithoutIndexedSource_skipsPinReconstruction()
      throws Exception {
    byte[] content = "orphaned hardlink".getBytes(StandardCharsets.UTF_8);
    Digest fileDigest = DIGEST_UTIL.compute(ByteString.copyFrom(content));
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("a")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .addFiles(
                FileNode.newBuilder()
                    .setName("b")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    Files.createDirectories(dirPath);
    Files.write(dirPath.resolve("a"), content);
    Files.createLink(dirPath.resolve("b"), dirPath.resolve("a"));
    assertThat(Files.getAttribute(dirPath.resolve("a"), "unix:nlink")).isEqualTo(2);

    getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
    Entry directoryEntry = storage.get(fileCache.getDirectoryKey(dirDigest));
    assertThat(directoryEntry).isNotNull();
    assertThat(directoryEntry.size)
        .isAtLeast(CASFileCache.estimateSizeOnDisk(content.length, 4096, /* isHardlink= */ false));
  }

  @Test
  public void startupScan_invalidDirectoryDoesNotApplyPendingHardlinkPins() throws Exception {
    byte[] content = "valid standalone source".getBytes(StandardCharsets.UTF_8);
    Digest fileDigest = DIGEST_UTIL.compute(ByteString.copyFrom(content));
    String fileKey = CASFileCache.getKey(fileDigest, false);
    Path standalone = fileCache.getPath(fileKey);
    Files.write(standalone, content);

    Digest dirDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("invalid hardlink dir"));
    String dirKey = fileCache.getDirectoryKey(dirDigest);
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    Files.createDirectories(dirPath);
    Files.createLink(dirPath.resolve("hardlink"), standalone);
    Files.write(dirPath.resolve("oversized"), new byte[(int) fileCache.maxEntrySize() + 1]);

    getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

    Entry source = storage.get(fileKey);
    assertThat(source).isNotNull();
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(storage.get(dirKey)).isNull();
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class JimfsDirectoryEntryCFCTest extends DirectoryEntryCFCTest {
    public JimfsDirectoryEntryCFCTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null));
    }
  }

  // Native filesystem test variant (needed for computeDirectory which uses real dir sizes)
  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class NativeDirectoryEntryCFCTest extends DirectoryEntryCFCTest {
    private final Path tempDir;

    public NativeDirectoryEntryCFCTest() throws IOException {
      this(createTempDirectory());
    }

    private NativeDirectoryEntryCFCTest(Path tempDir) {
      super(tempDir);
      this.tempDir = tempDir;
    }

    @Override
    protected boolean startupAdmitsExistingDirectoryTrees() {
      return true;
    }

    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      return Files.createTempDirectory("native-dir-entry-cfc-test");
    }

    @After
    @Override
    public void tearDown() throws IOException, InterruptedException {
      super.tearDown();
      Files.deleteIfExists(tempDir);
    }

    @Test
    public void computeDirectoryIncludesDirectoryOverhead() throws Exception {
      byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
      // The key must go through the entry path strategy (hex bucket directories).
      String dirKey = fileCache.getDirectoryKey(DIGEST_UTIL.compute(ByteString.copyFrom(content)));
      Path dirEntry = fileCache.getPath(dirKey);
      Files.createDirectories(dirEntry.resolve("subdir"));
      Files.write(dirEntry.resolve("file.txt"), content);

      getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

      Entry entry = storage.get(dirKey);
      assertThat(entry).isNotNull();
      // On a real filesystem, directory attrs.size() returns at least one block (typically 4096).
      // The entry size should be greater than just the file content because it includes
      // the directory overhead for both the root _dir directory and the "subdir" subdirectory.
      assertThat(entry.size).isGreaterThan((long) content.length);
    }
  }
}
