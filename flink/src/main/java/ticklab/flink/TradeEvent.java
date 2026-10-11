package ticklab.flink;

import java.math.BigDecimal;
import java.time.Instant;
import ticklab.v1.Exchange;
import ticklab.v1.Side;
import ticklab.v1.Source;
import ticklab.v1.Trade;

/**
 * One trade inside the jobs: the Avro {@link Trade} from the {@code trades} topic, flattened
 * into a plain Flink POJO (public fields and a no-argument constructor), plus the Kafka
 * timestamp at which it was published.
 */
public class TradeEvent {
    /** {@code exchange:symbol}, the key of every keyed step: one feed. */
    public String feed;

    public String exchange;
    public String symbol;
    public String base;
    public String quote;
    public long tradeId;
    public BigDecimal price;
    public BigDecimal quantity;
    public boolean takerBuy;
    /** When the exchange matched the trade, in microseconds since the epoch: event time. */
    public long tradeTimeMicros;
    /** When the producer received it, in microseconds; -1 for history, which never arrived live. */
    public long receivedTimeMicros;

    public String source;
    /** The Kafka timestamp: when the trade was published to the topic, in milliseconds. */
    public long publishTimeMillis;

    public TradeEvent() {}

    public static TradeEvent from(Trade trade, long publishTimeMillis) {
        TradeEvent event = new TradeEvent();
        event.exchange = trade.getExchange().name();
        event.symbol = trade.getSymbol();
        event.feed = event.exchange + ":" + event.symbol;
        event.base = trade.getBase();
        event.quote = trade.getQuote();
        event.tradeId = trade.getTradeId();
        event.price = trade.getPrice();
        event.quantity = trade.getQuantity();
        event.takerBuy = trade.getTakerSide() == Side.BUY;
        event.tradeTimeMicros = micros(trade.getTradeTime());
        event.receivedTimeMicros =
                trade.getReceivedTime() == null ? -1 : micros(trade.getReceivedTime());
        event.source = trade.getSource().name();
        event.publishTimeMillis = publishTimeMillis;
        return event;
    }

    /** Event time in milliseconds, the resolution of Flink's timestamps and watermarks. */
    public long eventTimeMillis() {
        return Math.floorDiv(tradeTimeMicros, 1000);
    }

    public Trade toAvro() {
        return Trade.newBuilder()
                .setExchange(Exchange.valueOf(exchange))
                .setSymbol(symbol)
                .setBase(base)
                .setQuote(quote)
                .setTradeId(tradeId)
                .setPrice(Decimals.exact(price))
                .setQuantity(Decimals.exact(quantity))
                .setTakerSide(takerBuy ? Side.BUY : Side.SELL)
                .setTradeTime(instant(tradeTimeMicros))
                .setReceivedTime(receivedTimeMicros < 0 ? null : instant(receivedTimeMicros))
                .setSource(Source.valueOf(source))
                .build();
    }

    static long micros(Instant instant) {
        return Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000);
    }

    static Instant instant(long micros) {
        return Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000);
    }

    @Override
    public String toString() {
        return feed + "#" + tradeId + "@" + tradeTimeMicros;
    }
}
