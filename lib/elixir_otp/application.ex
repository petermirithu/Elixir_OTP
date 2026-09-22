defmodule ElixirOtp.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      ElixirOtpWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:elixir_otp, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: ElixirOtp.PubSub},
      # Start a worker by calling: ElixirOtp.Worker.start_link(arg)
      # {ElixirOtp.Worker, arg},
      # Start to serve requests, typically the last entry
      ElixirOtpWeb.Endpoint
    ]

    # See https://elixir.hexdocs.pm/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ElixirOtp.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    ElixirOtpWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
