# Codemap

Bolster is roughly organized into several layers

- Outermost: `cli.rs` provides the CLI interface -- defining subcommands, flags, etc. and doing basic error-handling on them, along with ingesting the bolster config
- Intermediate: `commands.rs` contains the main logic for the subcommands (upload, download, ls)
- Innermost: The `api` module houses logic for talking to web services
  - `api/storage.rs` interacts with cloud storage (e.g. S3) such as upload and download
  - `api/datasets.rs` interacts with the datasets API/database such as creating and listing datasets and files

Other notable files:
- `app_config.rs` contains structures for deserializing the bolster config
- `models.rs` contains structures for deserializing datasets API responses


# Async

Bolster uses async because:
- Some dependency crates such as rusoto require async
- Downloads and uploads (of files and/or file chunks) can be more performant when concurrent
  - More concretely, a single-threaded uploader would read a chunk of the file, upload it to cloud storage, and wait for a response before continuing to the next chunk. An async (or multi-threaded) uploader can perform many of these actions concurrently, reducing the impact of the "do-nothing" time while waiting for a server response. Given the heavy-IO nature of the workload, async seems a better fit than multi-threading.

For async, bolster uses [tokio](https://docs.rs/tokio/1.6.1/tokio/). (Rusoto depends on tokio, which motivated the choice to use tokio over another async option, like async-std.)

The only areas of bolster that use async to add concurrency (i.e. there are multiple tasks running at the same time) are:
- When downloading multiple files
- When uploading multiple files
- When uploading a large file (i.e. a multipart/chunked upload)

One concern with async is balancing resource usage. A naive implementation of an async uploader might have many tasks that do `read file chunk -> upload chunk`, which might read the whole file into memory before receiving any `upload chunk` responses that let it release memory. To avoid this problem, bolster uses [`buffer_unordered`](https://docs.rs/futures/0.3.15/futures/stream/trait.StreamExt.html#method.buffer_unordered) and [`FuturesUnordered`](https://docs.rs/futures/0.3.15/futures/stream/struct.FuturesUnordered.html) as mechanisms for limiting the number of tasks in flight at a time, thereby also limiting RAM usage.

Bolster uses the default tokio scheduler, which uses a [thread pool](https://docs.rs/tokio/1.6.1/tokio/runtime/index.html#multi-thread-scheduler) and [work-stealing](https://tokio.rs/blog/2019-10-scheduler#work-stealing-scheduler). This means that using (a) streams with buffer_unordered or (b) spawning tasks and awaiting them as a group with FuturesUnordered will let tokio drive those tasks to completion as quickly and efficiently as possible. An alternate approach could be (1) spawn worker tasks, (2) create tasks of "work", like uploading file chunks, (3) distribute the "work" tasks to the workers using channels, (4) wait until all tasks are complete; this approach is less preferable, because a single worker could get unlucky and have repeated slow server responses, which could lead to that worker having a backlog of tasks while all other workers have completed their tasks and are sitting idle. Giving tasks/streams more directly to the tokio runtime and scheduler is better.


# Error Handling

In most cases, bolster propagates errors all the way out of the program, causing the program to exit and showing an error message to the user. Extra context/explanation can be attached to errors with anyhow's [with_context](https://docs.rs/anyhow/1.0.40/anyhow/trait.Context.html).

Some errors (such as server timeouts or HTTP 500 errors) could be retried, rather than raising those errors to the user. Retry functionality does not (currently) exist in bolster.