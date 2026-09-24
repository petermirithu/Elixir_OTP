defmodule ElixirOtp.JobQueue do
  use GenServer
  alias ElixirOtp.JobStore

  @server_name :job_queue

  defstruct job_ids: :queue.new()

  # client
  def start_link(_args) do
    GenServer.start_link(__MODULE__, %{}, name: @server_name)
  end

  def add_job(job_title) do
    GenServer.call(@server_name, {:enqueue, job_title})
  end

  def get_job_to_process() do
    GenServer.call(@server_name, :pop_job)
  end

  def get_pending_jobs() do
    GenServer.call(@server_name, :pending_jobs)
  end

  # callbacks
  @impl true
  def init(_state) do
    state = %__MODULE__{
      job_ids: :queue.new()
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

    JobStore.add_or_update_job(job)

    new_queue = :queue.in(job.id, state.job_ids)

    {:reply, {:ok, job}, %{state | job_ids: new_queue}}
  end

  @impl true
  def handle_call(:pending_jobs, _from, state) do
    job_ids = :queue.to_list(state.job_ids)
    {:reply, job_ids, state}
  end

  @impl true
  def handle_call(:pop_job, _from, state) do
    case :queue.out(state.job_ids) do
      {:empty, _} ->
        {:reply, :empty, state}

      {{:value, job_id}, remaining} ->
        {:reply, {:ok, job_id}, %{state | job_ids: remaining}}
    end
  end
end
