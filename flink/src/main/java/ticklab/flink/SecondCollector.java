package ticklab.flink;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Gathers one feed's trades of each second, in trade-ID order. Prices become whole cents: an
 * exception if one isn't, since every pair tested trades in cents.
 */
class SecondCollector extends ProcessWindowFunction<TradeEvent, SecondOfTrades, String, TimeWindow> {
    @Override
    public void process(String feed, Context context, Iterable<TradeEvent> window,
            Collector<SecondOfTrades> out) {
        List<TradeEvent> trades = new ArrayList<>();
        window.forEach(trades::add);
        trades.sort(Comparator.comparingLong(trade -> trade.tradeId));
        SecondOfTrades second = new SecondOfTrades();
        second.base = trades.get(0).base;
        second.secondMillis = context.window().getStart();
        int n = trades.size();
        second.ids = new long[n];
        second.timesMicros = new long[n];
        second.priceCents = new long[n];
        for (int i = 0; i < n; i++) {
            TradeEvent trade = trades.get(i);
            second.ids[i] = trade.tradeId;
            second.timesMicros[i] = trade.tradeTimeMicros;
            second.priceCents[i] = trade.price.movePointRight(2).longValueExact();
        }
        out.collect(second);
    }
}
