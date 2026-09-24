defmodule ElixirOtpWeb.HomeLive.Index do
  alias ElixirOtp.JobStore
  alias ElixirOtp.PubSub

  use ElixirOtpWeb, :live_view

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(PubSub, "job_updates")
    end

    jobs = JobStore.fetch_all()

    {
      :ok,
      socket
      |> populate_jobs_stats(jobs)
    }
  end

  @impl true
  def handle_info({:update_job, job}, socket) do
    updated_jobs =
      socket.assigns.jobs
      |> get_job_index(job.id)
      |> update_job_records(socket.assigns.jobs, job)

    {
      :noreply,
      socket
      |> populate_jobs_stats(updated_jobs)
    }
  end

  # private functions

  defp get_job_index(jobs, job_id) do
    Enum.find_index(jobs, &(&1.id == job_id))
  end

  defp update_job_records(index, jobs, job) when is_nil(index) == true do
    jobs ++ [job]
  end

  defp update_job_records(index, jobs, updated_job) when is_nil(index) == false do
    List.update_at(jobs, index, fn job ->
      job
      |> Map.put(:status, updated_job.status)
      |> Map.put(:finished_at, updated_job.finished_at)
      |> Map.put(:timeout, updated_job.timeout)
      |> Map.put(:retries, updated_job.retries)
    end)
  end

  defp populate_jobs_stats(socket, jobs) do
    socket
    |> assign(jobs: jobs)
    |> assign(jobs_queued: Enum.count(jobs, &(&1.status == :pending)))
    |> assign(jobs_running: Enum.count(jobs, &(&1.status == :running or &1.status == :retrying)))
    |> assign(jobs_completed: Enum.count(jobs, &(&1.status == :completed)))
    |> assign(jobs_failed: Enum.count(jobs, &(&1.status == :failed)))
    |> assign(
      jobs_to_be_retried: Enum.count(jobs, fn job -> job.status in [:retryable, :deferred] end)
    )
  end

  def format_date(%DateTime{} = date), do: Calendar.strftime(date, "%d %b %Y %H:%M")
  def format_date(%NaiveDateTime{} = date), do: Calendar.strftime(date, "%d %b %Y %H:%M")
  def format_date(nil), do: "---"
end
