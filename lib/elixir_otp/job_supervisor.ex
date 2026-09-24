defmodule ElixirOtp.JobSupervisor do
  use Supervisor

  def start_link(name) do
    Supervisor.start_link(__MODULE__, :ok, name: name)
  end

  # callbacks
  def init(_init_args) do
    children = [
      ElixirOtp.JobQueue,
      ElixirOtp.JobProcessor,
      {Task.Supervisor, name: :worker_supervisor}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
