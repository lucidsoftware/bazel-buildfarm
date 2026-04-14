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

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.CASFileCache.Entry;
import build.buildfarm.cas.cfc.DirectoryEntryCFC;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CFCLinkExecFileSystemTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);
  private static final Directory EMPTY_DIR = Directory.getDefaultInstance();
  private static final Digest EMPTY_DIR_DIGEST = DIGEST_UTIL.compute(EMPTY_DIR);
  private static final build.bazel.remote.execution.v2.Digest EMPTY_DIR_DIGEST_V2 =
      DigestUtil.toDigest(EMPTY_DIR_DIGEST);

  private Path root;
  private CASFileCache fileCache;
  private CFCLinkExecFileSystem execFileSystem;
  private ExecutorService service;

  @Before
  public void setUp() throws Exception {
    root =
        Iterables.getFirst(
            Jimfs.newFileSystem(
                    Configuration.unix().toBuilder()
                        .setAttributeViews("basic", "owner", "posix", "unix")
                        .build())
                .getRootDirectories(),
            null);

    Path cacheRoot = root.resolve("cache");
    Path execRoot = root.resolve("exec");
    Files.createDirectories(cacheRoot);
    Files.createDirectories(execRoot);

    service = newSingleThreadExecutor();

    ContentAddressableStorage delegate = mock(ContentAddressableStorage.class);
    when(delegate.getWrite(
            any(Compressor.Value.class),
            any(Digest.class),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(new NullWrite());
    doAnswer(invocation -> invocation.getArguments()[0])
        .when(delegate)
        .findMissingBlobs(any(Iterable.class), any(DigestFunction.Value.class));

    ConcurrentMap<String, Entry> storage = Maps.newConcurrentMap();
    fileCache =
        new DirectoryEntryCFC(
            cacheRoot,
            /* maxSizeInBytes= */ 1024 * 1024,
            /* maxEntrySizeInBytes= */ 1024 * 1024,
            /* hexBucketLevels= */ 0,
            service,
            /* accessRecorder= */ directExecutor(),
            storage,
            /* zstdBufferPool= */ null,
            /* onPut= */ digest -> {},
            /* onExpire= */ digests -> {},
            delegate,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> ByteString.EMPTY.newInput());
    fileCache.initializeRootDirectory();

    execFileSystem =
        new CFCLinkExecFileSystem(
            execRoot,
            fileCache,
            ImmutableMap.of(),
            /* linkInputDirectories= */ true,
            ImmutableList.of(".*"),
            /* allowSymlinkTargetAbsolute= */ false,
            service,
            /* accessRecorder= */ service,
            /* fetchService= */ service);
    // start() sets fileStore (needed for destroyExecDir) and initializes the cache
    execFileSystem.start(digests -> {}, /* skipLoad= */ true, /* writable= */ true).get();
  }

  @After
  public void tearDown() throws Exception {
    shutdownAndAwaitTermination(service, 1, SECONDS);
    // Clean up children of root, not root itself (Jimfs can't delete /)
    try (var stream = Files.list(root)) {
      var fileStore = Files.getFileStore(root);
      stream.forEach(
          child -> {
            try {
              Directories.remove(child, fileStore);
            } catch (Exception e) {
              // swallow — a failed cleanup shouldn't mask a real assertion failure
            }
          });
    }
  }

  private Path runCreateExecDir(
      String operationName,
      Digest inputRootDigest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Command command)
      throws Exception {
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(inputRootDigest)).build();
    return execFileSystem.createExecDir(
        operationName,
        directoriesIndex,
        DigestFunction.Value.SHA256,
        action,
        command,
        /* owner= */ null,
        WorkerExecutedMetadata.newBuilder());
  }

  /**
   * Verifies that directories listed in output_paths are not symlinked to the read-only CAS cache,
   * even when they match the linkedInputDirectories patterns. Before the fix, output_paths entries
   * were indistinguishable from regular input directories, causing them to be symlinked to
   * read-only CAS cache and making the action fail with AccessDeniedException.
   */
  @Test
  public void outputPathDirectoryIsNotSymlinked() throws Exception {
    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("outdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir, EMPTY_DIR_DIGEST_V2, EMPTY_DIR);

    Command command = Command.newBuilder().addOutputPaths("outdir").build();
    Path execDir = runCreateExecDir("test-op", rootDirDigest, directoriesIndex, command);

    try {
      Path outdir = execDir.resolve("outdir");
      assertThat(Files.exists(outdir)).isTrue();
      assertThat(Files.isSymbolicLink(outdir)).isFalse();
      assertThat(Files.isDirectory(outdir)).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  /**
   * Verifies output path exclusion works when the command has a non-empty working directory. Output
   * paths are relative to the working directory, so the resolution must account for
   * execDir.resolve(workingDirectory).resolve(outputPath).
   */
  @Test
  public void outputPathWithWorkingDirectoryIsNotSymlinked() throws Exception {
    Directory buildDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("outdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest buildDirDigest = DIGEST_UTIL.compute(buildDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("build")
                    .setDigest(DigestUtil.toDigest(buildDirDigest)))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest),
            rootDir,
            DigestUtil.toDigest(buildDirDigest),
            buildDir,
            EMPTY_DIR_DIGEST_V2,
            EMPTY_DIR);

    Command command =
        Command.newBuilder().setWorkingDirectory("build").addOutputPaths("outdir").build();
    Path execDir = runCreateExecDir("test-op-workdir", rootDirDigest, directoriesIndex, command);

    try {
      Path outdir = execDir.resolve("build/outdir");
      assertThat(Files.exists(outdir)).isTrue();
      assertThat(Files.isSymbolicLink(outdir)).isFalse();
      assertThat(Files.isDirectory(outdir)).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  /**
   * Verifies that multiple output_paths entries are all excluded from linking, while non-output
   * directories in the same tree are still symlinked.
   */
  @Test
  public void multipleOutputPathsAreNotSymlinked() throws Exception {
    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("classes").setDigest(EMPTY_DIR_DIGEST_V2))
            .addDirectories(
                DirectoryNode.newBuilder().setName("libdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .addDirectories(
                DirectoryNode.newBuilder().setName("resources").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir, EMPTY_DIR_DIGEST_V2, EMPTY_DIR);

    Command command =
        Command.newBuilder().addOutputPaths("classes").addOutputPaths("resources").build();
    Path execDir = runCreateExecDir("test-op-multi", rootDirDigest, directoriesIndex, command);

    try {
      assertThat(Files.isSymbolicLink(execDir.resolve("classes"))).isFalse();
      assertThat(Files.isDirectory(execDir.resolve("classes"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("resources"))).isFalse();
      assertThat(Files.isDirectory(execDir.resolve("resources"))).isTrue();
      assertThat(Files.isSymbolicLink(execDir.resolve("libdir"))).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  /**
   * Verifies that directories NOT in output_paths are still symlinked normally. This ensures the
   * fix doesn't break the linking optimization for regular input directories.
   */
  @Test
  public void nonOutputPathDirectoryIsStillSymlinked() throws Exception {
    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder().setName("inputdir").setDigest(EMPTY_DIR_DIGEST_V2))
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir, EMPTY_DIR_DIGEST_V2, EMPTY_DIR);

    Command command = Command.newBuilder().addOutputPaths("something_else").build();
    Path execDir = runCreateExecDir("test-op-non-output", rootDirDigest, directoriesIndex, command);

    try {
      Path inputdir = execDir.resolve("inputdir");
      assertThat(Files.exists(inputdir)).isTrue();
      assertThat(Files.isSymbolicLink(inputdir)).isTrue();
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }
  // --- createLightweightExecDir tests ---

  @Test
  public void createLightweightExecDir_createsOutputDirsOnly() throws Exception {
    Command command = Command.newBuilder().addOutputPaths("bazel-out/output.jar").build();

    Path execDir = execFileSystem.createLightweightExecDir("test-lightweight", command);

    try {
      assertThat(Files.isDirectory(execDir)).isTrue();
      // Output parent directory should be stamped
      assertThat(Files.isDirectory(execDir.resolve("bazel-out"))).isTrue();
      // No symlinks — lightweight exec dir has no input links
      try (var stream = Files.walk(execDir)) {
        long symlinkCount = stream.filter(Files::isSymbolicLink).count();
        assertThat(symlinkCount).isEqualTo(0);
      }
    } finally {
      execFileSystem.destroyExecDir(execDir);
    }
  }

  @Test
  public void createLightweightExecDir_destroyIsInexpensive() throws Exception {
    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();

    Path execDir = execFileSystem.createLightweightExecDir("test-lightweight-destroy", command);
    // destroyExecDir on lightweight dir should not try to decrement CAS refs
    // (no refs tracked in rootInputFiles/rootInputDirectories maps)
    execFileSystem.destroyExecDir(execDir);

    assertThat(Files.exists(execDir)).isFalse();
  }

  @Test
  public void createLightweightExecDir_cleansPartialDirWhenStampFails() {
    String operationName = "test-lightweight-stamp-failure";
    Command command = Command.newBuilder().addOutputPaths("bad\0path/output.jar").build();

    assertThrows(
        RuntimeException.class,
        () -> execFileSystem.createLightweightExecDir(operationName, command));

    assertThat(Files.exists(root.resolve("exec").resolve(operationName))).isFalse();
  }

  // --- fetchAndRefInputs tests ---

  @Test
  public void fetchAndRefInputs_returnsNonNullResult() throws Exception {
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());

    try {
      assertThat(fetchResult).isNotNull();
      assertThat(fetchResult.entries()).isNotEmpty();
      // src/ matches the whitelist and does not overlap the output, so it is a DIRECTORY entry.
      assertThat(fetchResult.entries()).hasSize(1);
      assertThat(fetchResult.entries().get(0).type())
          .isEqualTo(build.buildfarm.worker.persistent.FetchResult.EntryType.DIRECTORY);
      assertThat(fetchResult.entries().get(0).relativePath()).isEqualTo("src");
      assertThat(fetchResult.entries().get(0).casPath()).isNotNull();
    } finally {
      fetchResult.close();
    }
  }

  @Test
  public void fetchAndRefInputs_directoryMissAddsFetchedBytes() throws Exception {
    ByteString content = ByteString.copyFromUtf8("directory input content");
    fileCache.put(new ContentAddressableStorage.Blob(content, DIGEST_UTIL));
    Digest fileDigest = DIGEST_UTIL.compute(content);

    Directory srcDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("Input.java")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest srcDirDigest = DIGEST_UTIL.compute(srcDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(srcDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(srcDirDigest), srcDir);

    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();
    WorkerExecutedMetadata.Builder workerExecutedMetadata = WorkerExecutedMetadata.newBuilder();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            workerExecutedMetadata);

    try {
      assertThat(workerExecutedMetadata.getFetchedBytes()).isEqualTo(content.size());
    } finally {
      fetchResult.close();
    }
  }

  @Test
  public void fetchAndRefInputs_doesNotCreateAnyLinks() throws Exception {
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("lib")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    // Record the exec root state before fetchAndRefInputs
    Path execRoot = root.resolve("exec");

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());

    try {
      // No links should be created anywhere in the exec root
      // No symlinks in exec root
      try (var stream = Files.walk(execRoot)) {
        long symlinkCount = stream.filter(Files::isSymbolicLink).count();
        assertThat(symlinkCount).isEqualTo(0);
      }
    } finally {
      fetchResult.close();
    }
  }

  @Test
  public void fetchAndRefInputs_outputPathsDetermineGranularity() throws Exception {
    // Build tree: root -> {src (safe), out (has output)}
    // src should be a DIRECTORY entry, files in out should be individual FILE entries
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("out")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir);

    // Output is under out/, so it stays real while whitelisted src/ can be linked as a unit.
    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());

    try {
      // src/ is safe to symlink → DIRECTORY entry
      // out/ overlaps the output and is descended into (but empty, so no file entries).
      boolean hasSrcDir =
          fetchResult.entries().stream()
              .anyMatch(
                  e ->
                      e.relativePath().equals("src")
                          && e.type()
                              == build.buildfarm.worker.persistent.FetchResult.EntryType.DIRECTORY);
      assertThat(hasSrcDir).isTrue();

      // out/ should not appear as a DIRECTORY entry because it overlaps the output.
      boolean hasOutDir =
          fetchResult.entries().stream()
              .anyMatch(
                  e ->
                      e.relativePath().equals("out")
                          && e.type()
                              == build.buildfarm.worker.persistent.FetchResult.EntryType.DIRECTORY);
      assertThat(hasOutDir).isFalse();

      // out/ was descended into (excluded as an output-path ancestor), so it is recorded for
      // cleanup
      assertThat(fetchResult.descendedDirectories()).contains("out");
    } finally {
      fetchResult.close();
    }
  }

  /**
   * Key regression test for FetchRefVisitor. Tool inputs should be fetched into CAS and appear in
   * toolInputCasPaths(), but NOT in entries() (they are managed through the tool root mechanism).
   * Without the FetchRefVisitor fix, tool inputs were completely skipped — never fetched, never
   * recorded — causing copyToolInputsIntoWorkerToolRoot to fail.
   */
  @Test
  public void fetchAndRefInputs_toolInputsFetchedButNotInEntries() throws Exception {
    // Pre-populate CAS with tool input content
    ByteString toolContent = ByteString.copyFromUtf8("tool binary content");
    ContentAddressableStorage.Blob toolBlob =
        new ContentAddressableStorage.Blob(toolContent, DIGEST_UTIL);
    fileCache.put(toolBlob);
    Digest toolDigest = DIGEST_UTIL.compute(toolContent);
    build.bazel.remote.execution.v2.Digest toolReapiDigest = DigestUtil.toDigest(toolDigest);

    // Build tree: root -> {src/ (directory), tools/tool_binary (file)}
    Directory emptyDir = Directory.getDefaultInstance();
    Digest emptyDirDigest = DIGEST_UTIL.compute(emptyDir);

    Directory toolsDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("tool_binary")
                    .setDigest(toolReapiDigest)
                    .setIsExecutable(true)
                    .build())
            .build();
    Digest toolsDirDigest = DIGEST_UTIL.compute(toolsDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("src")
                    .setDigest(DigestUtil.toDigest(emptyDirDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("tools")
                    .setDigest(DigestUtil.toDigest(toolsDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(emptyDirDigest), emptyDir,
            DigestUtil.toDigest(toolsDirDigest), toolsDir);

    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    // "tools/tool_binary" is a tool input — should be fetched but not in entries
    ImmutableSet<String> toolInputPaths = ImmutableSet.of("tools/tool_binary");

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            toolInputPaths,
            WorkerExecutedMetadata.newBuilder());

    try {
      // (a) Tool input should be in toolInputCasPaths with a non-null CAS path
      assertThat(fetchResult.toolInputCasPaths()).containsKey("tools/tool_binary");
      assertThat(fetchResult.toolInputCasPaths().get("tools/tool_binary")).isNotNull();

      // (b) Tool input should NOT appear in entries (managed via tool root)
      boolean hasToolInEntries =
          fetchResult.entries().stream()
              .anyMatch(e -> e.relativePath().equals("tools/tool_binary"));
      assertThat(hasToolInEntries).isFalse();

      // (c) Non-tool directory (src/) should still be in entries as a DIRECTORY entry
      boolean hasSrcDir =
          fetchResult.entries().stream()
              .anyMatch(
                  e ->
                      e.relativePath().equals("src")
                          && e.type()
                              == build.buildfarm.worker.persistent.FetchResult.EntryType.DIRECTORY);
      assertThat(hasSrcDir).isTrue();
    } finally {
      fetchResult.close();
    }
  }

  /**
   * Regression test: zero-size tool inputs (e.g. _repo_mapping in .runfiles) have no CAS entry.
   * They must be tracked in zeroSizeToolInputPaths so copyToolInputsIntoWorkerToolRoot can create
   * them as empty files. Without the fix, zero-size tool inputs were silently dropped — not in
   * toolInputCasPaths, not in entries, not tracked anywhere.
   */
  @Test
  public void fetchAndRefInputs_zeroSizeToolInputTrackedInFetchResult() throws Exception {
    // Zero-size digest
    ByteString emptyContent = ByteString.EMPTY;
    Digest zeroDigest = DIGEST_UTIL.compute(emptyContent);
    build.bazel.remote.execution.v2.Digest zeroReapiDigest = DigestUtil.toDigest(zeroDigest);

    // Non-zero tool input (to verify both paths work together)
    ByteString toolContent = ByteString.copyFromUtf8("tool binary content");
    ContentAddressableStorage.Blob toolBlob =
        new ContentAddressableStorage.Blob(toolContent, DIGEST_UTIL);
    fileCache.put(toolBlob);
    Digest toolDigest = DIGEST_UTIL.compute(toolContent);
    build.bazel.remote.execution.v2.Digest toolReapiDigest = DigestUtil.toDigest(toolDigest);

    // Build tree: root -> tools/{tool_binary (non-zero), zero_file (zero-size)}
    Directory toolsDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("tool_binary")
                    .setDigest(toolReapiDigest)
                    .setIsExecutable(true)
                    .build())
            .addFiles(FileNode.newBuilder().setName("zero_file").setDigest(zeroReapiDigest).build())
            .build();
    Digest toolsDirDigest = DIGEST_UTIL.compute(toolsDir);

    Directory rootDir =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("tools")
                    .setDigest(DigestUtil.toDigest(toolsDirDigest))
                    .build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);

    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDirDigest), rootDir,
            DigestUtil.toDigest(toolsDirDigest), toolsDir);

    Command command = Command.newBuilder().addOutputPaths("out/output.jar").build();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    // Both files are tool inputs
    ImmutableSet<String> toolInputPaths = ImmutableSet.of("tools/tool_binary", "tools/zero_file");

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            toolInputPaths,
            WorkerExecutedMetadata.newBuilder());

    try {
      // Non-zero tool input in toolInputCasPaths
      assertThat(fetchResult.toolInputCasPaths()).containsKey("tools/tool_binary");

      // Zero-size tool input NOT in toolInputCasPaths (no CAS entry)
      assertThat(fetchResult.toolInputCasPaths()).doesNotContainKey("tools/zero_file");

      // Zero-size tool input tracked in zeroSizeToolInputPaths
      assertThat(fetchResult.zeroSizeToolInputPaths()).contains("tools/zero_file");

      // Neither tool input should appear in entries
      boolean hasToolInEntries =
          fetchResult.entries().stream()
              .anyMatch(
                  e ->
                      e.relativePath().equals("tools/tool_binary")
                          || e.relativePath().equals("tools/zero_file"));
      assertThat(hasToolInEntries).isFalse();
    } finally {
      fetchResult.close();
    }
  }

  // The fetch path must enforce the same absolute-symlink-target guard as createExecDir.
  @Test
  public void fetchAndRefInputs_absoluteSymlinkTargetRejected() throws Exception {
    Directory rootDir =
        Directory.newBuilder()
            .addSymlinks(
                SymlinkNode.newBuilder().setName("escape").setTarget("/etc/passwd").build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(rootDirDigest), rootDir);
    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    // Shared fixture is configured allowSymlinkTargetAbsolute=false, so the absolute target
    // rejects.
    assertThrows(
        IOException.class,
        () ->
            execFileSystem.fetchAndRefInputs(
                directoriesIndex,
                DigestFunction.Value.SHA256,
                action,
                command,
                ImmutableSet.of(),
                WorkerExecutedMetadata.newBuilder()));
  }

  // FetchRefVisitor must emit ZERO_SIZE_FILE and SYMLINK_NODE entries for non-tool size-0 files and
  // protobuf symlinks (producer side; the consumer side is covered in ProtoCoordinatorTest).
  @Test
  public void fetchAndRefInputs_emitsZeroSizeAndSymlinkEntries() throws Exception {
    Digest zeroDigest = DIGEST_UTIL.compute(ByteString.EMPTY);
    Directory rootDir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("empty.txt")
                    .setDigest(DigestUtil.toDigest(zeroDigest))
                    .build())
            .addSymlinks(SymlinkNode.newBuilder().setName("link").setTarget("empty.txt").build())
            .build();
    Digest rootDirDigest = DIGEST_UTIL.compute(rootDir);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(rootDirDigest), rootDir);
    Command command = Command.getDefaultInstance();
    Action action =
        Action.newBuilder().setInputRootDigest(DigestUtil.toDigest(rootDirDigest)).build();

    build.buildfarm.worker.persistent.FetchResult fetchResult =
        execFileSystem.fetchAndRefInputs(
            directoriesIndex,
            DigestFunction.Value.SHA256,
            action,
            command,
            ImmutableSet.of(),
            WorkerExecutedMetadata.newBuilder());
    try {
      assertThat(
              fetchResult.entries().stream()
                  .anyMatch(
                      e ->
                          e.relativePath().equals("empty.txt")
                              && e.type()
                                  == build.buildfarm.worker.persistent.FetchResult.EntryType
                                      .ZERO_SIZE_FILE))
          .isTrue();
      assertThat(
              fetchResult.entries().stream()
                  .anyMatch(
                      e ->
                          e.relativePath().equals("link")
                              && e.type()
                                  == build.buildfarm.worker.persistent.FetchResult.EntryType
                                      .SYMLINK_NODE
                              && "empty.txt".equals(e.symlinkTarget())))
          .isTrue();
    } finally {
      fetchResult.close();
    }
  }
}
