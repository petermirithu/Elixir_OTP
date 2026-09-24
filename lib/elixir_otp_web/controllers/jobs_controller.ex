defmodule ElixirOtpWeb.JobsController do
  use ElixirOtpWeb, :controller

  def queue(conn, %{"job_title" => job_title}) do
    with {:ok, job} <- ElixirOtp.JobQueue.add_job(job_title) do
      conn
      |> put_status(:created)
      |> json(%{job: job})
    end
  end
end
