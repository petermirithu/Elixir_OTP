defmodule ElixirOtp.JobStore do
  alias ElixirOtp.PubSub
  use GenServer

  @server_name :job_store
  @table_name :jobs_table

  # client api
  def start_link(_args) do
    GenServer.start_link(__MODULE__, %{}, name: @server_name)
  end

  def add_or_update_job(job) do
    :ets.insert(@table_name, {job.id, job})
    client_update_jobs(job)
    :ok
  end

  def get_job(job_id) do
    case :ets.lookup(@table_name, job_id) do
      [{^job_id, job}] -> job
      [] -> nil
    end
  end

  def fetch_all() do
    Enum.map(:ets.tab2list(@table_name), fn {_job_id, job} ->
      job
    end)
  end

  # callbacks
  @impl true
  def init(_init_arg) do
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    {:ok, %{}}
  end

  # private functions
  defp client_update_jobs(job) do
    Phoenix.PubSub.broadcast(
      PubSub,
      "job_updates",
      {:update_job, job}
    )
  end
end
