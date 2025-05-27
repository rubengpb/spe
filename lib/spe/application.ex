defmodule SPE.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Phoenix.PubSub, name: SPE.PubSub}
    ]
    opts = [strategy: :one_for_one, name: SPE.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
