defmodule ElixirOtp.JobProcessor do
  alias ElixirOtp.JobStore
  alias ElixirOtp.JobQueue
  use GenServer

  @server_name :job_processor
  @max_concurrent_tasks 5
  @max_retries 3
  @job_duration 15_000
  @default_timeout 5_000
  @max_timeout 30_000

  defstruct workers_running: []

  # client
  def start_link(_args) do
    GenServer.start_link(__MODULE__, %{}, name: @server_name)
  end

  # callbacks
  @impl true
  def init(_state) do
    # Start processing loop after a short delay to make sure everything is setup
    Process.send_after(self(), :tick, 1000)

    state = %__MODULE__{
      workers_running: []
    }

    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    new_state = process_next_job(state)

    interval = if length(new_state.workers_running) == 0, do: 2000, else: 0

    Process.send_after(self(), :tick, interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:retry_job, job_id}, state) do
    new_state = JobQueue.get_pending_jobs() |> process_failed_job(job_id, state)

    {:noreply, new_state}
  end

  @impl true
  def handle_info({worker_ref, result}, state) do
    # Worker completed job!
    Process.demonitor(worker_ref, [:flush])

    worker = Enum.find(state.workers_running, &(&1.worker_ref == worker_ref))

    case result do
      {:ok, _msg} ->
        update_job_status(worker.job_id, :completed)

      {:error, _reason} ->
        fail_or_retry_job(worker)
    end

    new_state = update_workers_running(:remove, worker, state)

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, worker_ref, :process, _pid, _reason}, state) do
    worker = Enum.find(state.workers_running, &(&1.worker_ref == worker_ref))

    fail_or_retry_job(worker)

    new_state = update_workers_running(:remove, worker, state)

    {:noreply, new_state}
  end

  # Private functions
  defp fail_or_retry_job(worker) do
    retries = get_job_retries(worker.job_id)

    cond do
      is_nil(retries) or retries >= @max_retries ->
        update_job_status(worker.job_id, :failed)

      true ->
        # retries < @max_retries
        update_job_status(worker.job_id, :retryable)

        send(self(), {:retry_job, worker.job_id})
    end
  end

  defp get_job_retries(job_id) do
    case JobStore.get_job(job_id) do
      nil ->
        nil

      job ->
        job.retries
    end
  end

  defp update_job_status(job_id, status) do
    case JobStore.get_job(job_id) do
      nil ->
        nil

      job ->
        update_job_by_status(job, status) |> JobStore.add_or_update_job()
    end
  end

  defp update_job_by_status(job, status) do
    case status do
      :running ->
        %{job | status: status}

      :retrying ->
        %{job | status: status, retries: job.retries + 1, finished_at: nil}

      _ ->
        %{job | status: status, finished_at: DateTime.utc_now()}
    end
  end

  defp exponential_backoff(retries) do
    next_timeout = @default_timeout * (:math.pow(2, retries - 1) |> round())
    if next_timeout > @max_timeout, do: @max_timeout, else: next_timeout
  end

  defp process_failed_job(pending_jobs, job_id, state)
       when length(pending_jobs) > 0 or length(state.workers_running) >= @max_concurrent_tasks do
    case JobStore.get_job(job_id) do
      nil ->
        state

      job ->
        next_timeout = Map.get(job, :retries) |> exponential_backoff()

        job |> Map.put(:timeout, next_timeout) |> JobStore.add_or_update_job()

        update_job_status(job.id, :deferred)

        Process.send_after(self(), {:retry_job, job.id}, next_timeout)

        state
    end
  end

  defp process_failed_job(_queue, job_id, state) do
    start_worker(job_id, :retrying, state)
  end

  defp process_next_job(%{workers_running: current} = state)
       when length(current) == @max_concurrent_tasks do
    state
  end

  defp process_next_job(state) do
    case ElixirOtp.JobQueue.get_job_to_process() do
      :empty ->
        state

      {:ok, job_id} ->
        start_worker(job_id, :running, state)
    end
  end

  defp start_worker(job_id, status, state) do
    worker =
      Task.Supervisor.async_nolink(:worker_supervisor, fn ->
        execute_job(job_id)
      end)

    update_job_status(job_id, status)
    update_workers_running(:add, %{worker_ref: worker.ref, job_id: job_id}, state)
  end

  defp update_workers_running(option, worker, state) do
    case option do
      :add ->
        %{state | workers_running: state.workers_running ++ [worker]}

      :remove ->
        %{state | workers_running: remove_worker(state.workers_running, worker.worker_ref)}
    end
  end

  defp remove_worker(workers_running, worker_ref) do
    Enum.reject(workers_running, &(&1.worker_ref == worker_ref))
  end

  defp execute_job(job_id) do
    Process.sleep(@job_duration)

    case JobStore.get_job(job_id) do
      nil ->
        {:error, "Job is missing!"}

      job ->
        number = :rand.uniform(100)

        if rem(number, 2) == 0 and not String.contains?(job.title, "fail") do
          {:ok, "Job processed successfully!"}
        else
          {:error, "We are simulating a failed job!"}
        end
    end
  end
end
