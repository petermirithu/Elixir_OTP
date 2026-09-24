defmodule ElixirOtp.JobProcessor do
  alias ElixirOtp.JobQueue
  use GenServer
  alias ElixirOtp.PubSub

  @max_concurrent_tasks 5
  @max_retries 3
  @default_timeout 10

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
    # Start processing loop after a short delay to make everything is setup
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

    interval = if length(new_state.workers_running) == 0, do: 1000, else: 500

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
    case jobs_queued() do
      false ->
        job = update_job_status(job, :retrying)

        worker =
          Task.Supervisor.async_nolink(:worker_supervisor, fn ->
            execute_job(job)
          end)

        new_state = update_workers_running(:add, %{worker_ref: worker.ref, job: job}, state)

        {:noreply, new_state}

      true ->
        new_state = handle_retry(job, state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({worker_ref, result}, state) do
    # Worker completed!
    Process.demonitor(worker_ref, [:flush])

    worker = Enum.find(state.workers_running, &(&1.worker_ref == worker_ref))

    case result do
      {:ok, _msg} ->
        update_job_status(worker.job, :completed)

      {:error, _reason} ->
        # Retry failed job
        if worker.job.retries < @max_retries do
          send(self(), {:retry_job, update_job_status(worker.job, :retried)})
        else
          update_job_status(worker.job, :failed)
        end
    end

    new_state = update_workers_running(:remove, worker, state)

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, worker_ref, :process, _pid, _reason}, state) do
    worker = Enum.find(state.workers_running, &(&1.worker_ref == worker_ref))

    if worker.job.retries < @max_retries do
      send(self(), {:retry_job, update_job_status(worker.job, :retried)})
    else
      update_job_status(worker.job, :failed)
    end

    new_state = update_workers_running(:remove, worker, state)

    {:noreply, new_state}
  end

  # Private functions
  defp jobs_queued() do
    case JobQueue.list_jobs() do
      [] ->
        false

      _ ->
        true
    end
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

  defp handle_retry(job, state) do
    next_timeout = Map.get(job, :timeout, @default_timeout) |> exponential_backoff()

    updated_job = Map.put(job, :timeout, next_timeout)

    Process.send_after(
      self(),
      {:retry_job, update_job_status(updated_job, :deferred)},
      next_timeout
    )

    state
  end

  defp exponential_backoff(timeout), do: :math.pow(timeout, 2) |> round()

  defp process_next_job(%{workers_running: current} = state)
       when length(current) == @max_concurrent_tasks do
    state
  end

  defp process_next_job(state) do
    case ElixirOtp.JobQueue.get_job_to_process() do
      :empty ->
        state

      {:ok, job} ->
        job = update_job_status(job, :running)

        worker =
          Task.Supervisor.async_nolink(:worker_supervisor, fn ->
            execute_job(job)
          end)

        update_workers_running(:add, %{worker_ref: worker.ref, job: job}, state)
    end
  end

  defp execute_job(job) do
    Process.sleep(5000)

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

  defp client_update_workers_running(workers_running) do
    Phoenix.PubSub.broadcast(
      PubSub,
      "job_updates",
      {:update_workers_running, length(workers_running)}
    )
  end
end
