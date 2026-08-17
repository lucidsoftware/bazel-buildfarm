load("@rules_java//java:java_test.bzl", "java_test")

COMMON_DEPS = [
    "//persistentworkers/src/main/java/persistent/common:persistent-common",
    "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
    "@buildfarm_maven//:com_google_truth_truth",
    "@buildfarm_maven//:commons_io_commons_io",
    "@buildfarm_maven//:org_apache_commons_commons_pool2",
    "@buildfarm_maven//:org_mockito_mockito_core",
]

java_test(
    name = "CommonsPoolTest",
    size = "small",
    srcs = ["processes/CommonsPoolTest.java"],
    deps = COMMON_DEPS,
)

java_test(
    name = "CoordinatorTest",
    size = "small",
    srcs = ["processes/CoordinatorTest.java"],
    deps = COMMON_DEPS,
)
