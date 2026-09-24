defmodule ElixirOtp.JobProcessor do
  alias ElixirOtp.JobQueue
  use GenServer
  alias ElixirOtp.PubSub

  @max_concurrent_tasks 5
  @max_retries 3
  @job_duration 15_000
  @default_timeout 5_000
  @max_timeout 30_000

  defstruct workers_running: [],
            jobs: []

  # client
  def start_link(_args) do
    GenServer.start_link(__MODULE__, %{}, name: :job_processor)
  end

  def get_processing_stats() do
    state = GenServer.call(:job_processor, :stats)

    %{
      workers_running: length(state.workers_running),
      jobs: state.jobs
    }
  end

  # callbacks
  @impl true
  def init(_state) do
    # Start processing loop after a short delay to make sure everything is setup
    Process.send_after(self(), :tick, 1000)

    state = %__MODULE__{
      workers_running: [],
      jobs: []
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_info(:tick, state) do
    new_state = process_next_job(state)

    interval = if length(new_state.workers_running) == 0, do: 2000, else: 0

    Process.send_after(self(), :tick, interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:update_job_record, updated_job}, state) do
    job_id = updated_job.id

    new_jobs =
      case Enum.any?(state.jobs, &(&1.id == job_id)) do
        true ->
          Enum.map(state.jobs, fn
            %{id: ^job_id} = _job ->
              updated_job

            job ->
              job
          end)

        false ->
          state.jobs ++ [updated_job]
      end

    client_update_jobs_list(new_jobs)

    {:noreply, %{state | jobs: new_jobs}}
  end

  @impl true
  def handle_info({:retry_job, job}, state) do
    new_state = JobQueue.list_jobs() |> process_failed_job(job, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({worker_ref, result}, state) do
    # Worker completed job!
    Process.demonitor(worker_ref, [:flush])

    worker = Enum.find(state.workers_running, &(&1.worker_ref == worker_ref))

    case result do
      {:ok, _msg} ->
        update_job_status(worker.job, :completed)

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
  defp fail_or_retry_job(worker) when worker.job.retries < @max_retries do
    send(self(), {:retry_job, update_job_status(worker.job, :retried)})
  end

  defp fail_or_retry_job(worker) do
    update_job_status(worker.job, :failed)
  end

  defp update_job_status(job, status) do
    updated_job =
      case status do
        :running ->
          %{job | status: status}

        :retried ->
          %{job | status: status, retries: job.retries + 1, finished_at: DateTime.utc_now()}

        :retrying ->
          %{job | status: status, finished_at: nil}

        _ ->
          %{job | status: status, finished_at: DateTime.utc_now()}
      end

    send(self(), {:update_job_record, updated_job})

    updated_job
  end

  defp remove_worker(workers_running, worker_ref) do
    Enum.reject(workers_running, &(&1.worker_ref == worker_ref))
  end

  defp exponential_backoff(retries) do
    next_timeout = @default_timeout * (:math.pow(2, retries - 1) |> round())
    if next_timeout > @max_timeout, do: @max_timeout, else: next_timeout
  end

  defp process_failed_job(queue, job, state)
       when length(queue) > 0 or length(state.workers_running) == @max_concurrent_tasks do
    next_timeout = Map.get(job, :retries) |> exponential_backoff()

    updated_job = update_job_status(Map.put(job, :timeout, next_timeout), :deferred)

    Process.send_after(self(), {:retry_job, updated_job}, next_timeout)

    state
  end

  defp process_failed_job(_queue, job, state) do
    start_worker(job, :retrying, state)
  end

  defp process_next_job(%{workers_running: current} = state)
       when length(current) == @max_concurrent_tasks do
    state
  end

  defp process_next_job(state) do
    case ElixirOtp.JobQueue.get_job_to_process() do
      :empty ->
        state

      {:ok, job} ->
        start_worker(job, :running, state)
    end
  end

  defp start_worker(job, status, state) do
    job = update_job_status(job, status)

    worker =
      Task.Supervisor.async_nolink(:worker_supervisor, fn ->
        execute_job(job)
      end)

    update_workers_running(:add, %{worker_ref: worker.ref, job: job}, state)
  end

  defp update_workers_running(option, worker, state) do
    new_state =
      case option do
        :add ->
          %{state | workers_running: state.workers_running ++ [worker]}

        :remove ->
          %{state | workers_running: remove_worker(state.workers_running, worker.worker_ref)}
      end

    client_update_workers_running(new_state.workers_running)
    new_state
  end

  defp execute_job(job) do
    Process.sleep(@job_duration)

    number = :rand.uniform(100)

    if rem(number, 2) == 0 and not String.contains?(job.title, "fail") do
      {:ok, "Job processed successfully!"}
    else
      {:error, "We are simulating a failed job!"}
    end
  end

  defp client_update_jobs_list(jobs) do
    Phoenix.PubSub.broadcast(
      PubSub,
      "job_updates",
      {:update_jobs_list, jobs}
    )
  end

  defp client_update_workers_running(workers_running) do
    Phoenix.PubSub.broadcast(
      PubSub,
      "job_updates",
      {:update_workers_running, length(workers_running)}
    )
  end
end
