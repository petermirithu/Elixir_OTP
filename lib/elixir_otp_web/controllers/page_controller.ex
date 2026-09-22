defmodule ElixirOtpWeb.PageController do
  use ElixirOtpWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
