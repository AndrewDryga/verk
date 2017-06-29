defmodule Verk.QueuesDrainer do
  defmodule Consumer do
    use GenStage

    def start_link() do
      GenStage.start_link(__MODULE__, self)
    end

    def init(parent) do
      filter = fn event -> event.__struct__ == Verk.Events.QueuePaused end
      {:consumer, parent, subscribe_to: [{Verk.EventProducer, selector: filter}]}
    end

    def handle_events([event], _from, parent) do
      send parent, {:paused, event.queue}
      {:noreply, [], parent}
    end
  end

  use GenServer
  require Logger

  def start_link() do
    GenServer.start_link(__MODULE__, :ok)
  end

  def init(_) do
    IO.puts "DRAINER IS ON"
    # filter = fn event -> event.__struct__ == Verk.Events.QueuePaused end
    # {:consumer, :ok, subscribe_to: [{Verk.EventProducer, selector: filter}]}
    Process.flag(:trap_exit, true)
    # {:consumer, :ok, []}
    {:ok, :ok}
  end

  def handle_info({:'EXIT', _pid, reason}, state) do
    Logger.warn "QueuesDrainer exiting"
    {:stop, reason, state}
  end

  # def handle_events([event], _from, parent) do
    # send parent, {:paused, event.queue}
    # {:noreply, [], parent}
  # end

  def terminate(_, _) do
    IO.puts "terminate!"
    queues = Confex.get_map(:verk, :queues, [])

    {:ok, pid} = Consumer.start_link

    queues = Enum.reduce(queues, MapSet.new, fn {queue, _}, queues ->
      case Verk.pause_queue(queue) do
        :ok -> MapSet.put(queues, queue)
        :already_paused -> queues
      end
    end)

    n_queues = MapSet.size(queues)
    IO.puts "waiting for #{n_queues} queues"

    for _x <- (1..n_queues) do
      receive do
        {:paused, queue} -> IO.puts "paused queue #{queue}!"
      end
    end

    IO.puts "graceful as fuck aye"
  end
end

