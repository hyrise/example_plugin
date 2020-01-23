# Hyrise Plugin for Extraction of Column Accesses from various Benchmarks

This benchmarks takes a given workload and creates a handful of CSV that contain system meta data (table sizes, encodings) and operator meta meta.
Focus has been the accesses to physical data for TPC-H. Hence, there is a lack of operations that are not important in the TPC-H (e.g., index scans, modifications) and we probably do not extract certain information that migh be of interest for other use cases than physical column accesses.

## Usage

Assuming you are using `ninja` and your build directory is `relwithdeb`:
`ninja -C relwithdeb Driver hyrisePlayground && BENCHMARK_TO_RUN=TPC-H ./relwithdeb/hyrisePlayground relwithdeb/libDriver.(dylib/so)`

The benchmark to execute is passed via an environment variable (current lack of passing information to plugins). Possible options are `TPC-H`, `TPC-DS`, and `JOB`.