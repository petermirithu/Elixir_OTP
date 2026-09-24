defmodule ElixirOtp.JobQueue do
  use GenServer
  alias ElixirOtp.PubSub

  defstruct jobs: :queue.new()

  # client
  def start_link(_args) do
    GenServer.start_link(__MODULE__, %{}, name: :job_queue)
  end

  def add_job(job_title) do
    GenServer.call(:job_queue, {:enqueue, job_title})
  end

  def get_job_to_process() do
    GenServer.call(:job_queue, :pop_job)
  end

  def list_jobs() do
    GenServer.call(:job_queue, :list_jobs)
  end

  # callbacks
  @impl true
  def init(_state) do
    state = %__MODULE__{
      jobs: :queue.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, job_title}, _from, state) do
    job = %{
      id: System.unique_integer([:monotonic, :positive]),
      title: job_title,
      status: :pending,
      finished_at: nil,
      retries: 0,
      timeout: 0
    }

    new_queue = :queue.in(job, state.jobs)

    client_update_queued_jobs(new_queue)

    {:reply, {:ok, job}, %{state | jobs: new_queue}}
  end

  @impl true
  def handle_call(:list_jobs, _from, state) do
    jobs = queue_jobs_as_list(state.jobs)
    {:reply, jobs, state}
  end

  @impl true
  def handle_call(:pop_job, _from, state) do
    case :queue.out(state.jobs) do
      {:empty, _} ->
        {:reply, :empty, state}

      {{:value, job}, remaining} ->
        client_update_queued_jobs(remaining)
        {:reply, {:ok, job}, %{state | jobs: remaining}}
    end
  end

  # Private functions
  defp queue_jobs_as_list(queue_jobs) do
    :queue.to_list(queue_jobs)
  end

  defp client_update_queued_jobs(jobs) do
    Phoenix.PubSub.broadcast(
      PubSub,
      "job_updates",
      {:update_queued_jobs, queue_jobs_as_list(jobs)}
    )
  end
end
