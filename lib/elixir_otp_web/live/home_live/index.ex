defmodule ElixirOtpWeb.HomeLive.Index do
  use ElixirOtpWeb, :live_view
  alias ElixirOtp.PubSub

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(PubSub, "job_updates")
      IO.inspect("Subscribed!!!!!!!!!... PubSub .............")
    end

    {jobs_queued, processing_stats} = fetch_jobs()

    IO.inspect(jobs_queued)
    IO.inspect(processing_stats)

    {
      :ok,
      socket
      |> populate_jobs_stats(
        jobs_queued,
        processing_stats.workers_running,
        processing_stats.jobs
      )
    }
  end

  @impl true
  def handle_info({:update_queued_jobs, jobs_queued}, socket) do
    {
      :noreply,
      socket
      |> populate_jobs_stats(
        jobs_queued,
        socket.assigns.workers_running,
        socket.assigns.processor_jobs
      )
    }
  end

  @impl true
  def handle_info({:update_workers_running, workers_running}, socket) do
    {
      :noreply,
      socket
      |> populate_jobs_stats(
        socket.assigns.jobs_queued,
        workers_running,
        socket.assigns.processor_jobs
      )
    }
  end

  @impl true
  def handle_info({:update_jobs_list, processor_jobs}, socket) do
    {
      :noreply,
      socket
      |> populate_jobs_stats(
        socket.assigns.jobs_queued,
        socket.assigns.workers_running,
        processor_jobs
      )
    }
  end

  defp fetch_jobs do
    jobs_queued = ElixirOtp.JobQueue.list_jobs()
    processing_stats = ElixirOtp.JobProcessor.get_processing_stats()

    {jobs_queued, processing_stats}
  end

  defp populate_jobs_stats(socket, jobs_queued, workers_running, processor_jobs) do
    socket
    |> assign(jobs: compile_jobs(jobs_queued, processor_jobs))
    |> assign(jobs_queued: jobs_queued)
    |> assign(workers_running: workers_running)
    |> assign(processor_jobs: processor_jobs)
    |> assign(jobs_completed: Enum.count(processor_jobs, &(&1.status == :completed)))
    |> assign(jobs_failed: Enum.count(processor_jobs, &(&1.status == :failed)))
    |> assign(
      jobs_retrying:
        Enum.count(processor_jobs, fn job -> job.status in [:retried, :deferred, :retrying] end)
    )
  end

  defp compile_jobs(jobs_queued, processor_jobs) do
    [] ++ jobs_queued ++ processor_jobs
  end

  def format_date(%DateTime{} = date), do: Calendar.strftime(date, "%d %b %Y %H:%M")
  def format_date(%NaiveDateTime{} = date), do: Calendar.strftime(date, "%d %b %Y %H:%M")
  def format_date(nil), do: "---"
end
