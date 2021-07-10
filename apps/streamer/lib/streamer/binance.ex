defmodule Streamer.Binance do
  use WebSockex
  alias Streamer.Binance.TradeEvent
  @stream_enpoint "wss://stream.binance.com:9443/ws/"

  def start_link(symbol, state) do
    WebSockex.start_link("#{@stream_enpoint}#{symbol}@trade", __MODULE__, state)
  end

  def handle_frame({type, msg}, state) do
    case Jason.decode(msg) do
      {:ok, event} -> handle_event(event, state)
      {:error, _} -> throw("Unable to parse msg: #{msg}")
    end

    {:ok, state}
  end

  def handle_event(%{"e" => "trade"} = event, state) do
    trade_event = %TradeEvent{
      :event_type => event["e"],
      :event_time => event["E"],
      :symbol => event["s"],
      :trade_id => event["t"],
      :price => event["p"],
      :quantity => event["q"],
      :buyer_order_id => event["b"],
      :seller_order_id => event["a"],
      :trade_time => event["T"],
      :buyer_market_maker => event["m"]
    }

    IO.inspect(trade_event, label: "Trade event received: ")
  end

  def handle_cast({:send, {type, msg} = frame}, state) do
    IO.puts("Sending #{type} frame with payload: #{msg}")
    {:reply, frame, state}
  end
end
