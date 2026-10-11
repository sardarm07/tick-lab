package ticklab.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class CandleAggregateTest {
    private static final CandleAggregate AGGREGATE = new CandleAggregate();

    private static TradeEvent trade(long id, String price, String quantity, boolean takerBuy) {
        TradeEvent trade = DedupTest.trade(id, 0);
        trade.price = new BigDecimal(price);
        trade.quantity = new BigDecimal(quantity);
        trade.takerBuy = takerBuy;
        return trade;
    }

    private static List<TradeEvent> minute() {
        return List.of(
                trade(10, "100.10", "0.5", true),
                trade(11, "100.30", "1.25", false),
                trade(12, "99.95", "0.001", true),
                trade(13, "100.00", "2", false));
    }

    private static CandleAccumulator build(List<TradeEvent> trades) {
        CandleAccumulator candle = AGGREGATE.createAccumulator();
        for (TradeEvent trade : trades) {
            candle = AGGREGATE.add(trade, candle);
        }
        return candle;
    }

    @Test
    void openAndCloseComeFromTradeIdsAndSumsAreExact() {
        CandleAccumulator candle = build(minute());
        assertThat(candle.open).isEqualByComparingTo("100.10");
        assertThat(candle.close).isEqualByComparingTo("100.00");
        assertThat(candle.high).isEqualByComparingTo("100.30");
        assertThat(candle.low).isEqualByComparingTo("99.95");
        assertThat(candle.volume).isEqualByComparingTo("3.751");
        // 100.10*0.5 + 100.30*1.25 + 99.95*0.001 + 100.00*2 = 375.52495
        assertThat(candle.quoteVolume).isEqualByComparingTo("375.52495");
        assertThat(candle.takerBuyVolume).isEqualByComparingTo("0.501");
        assertThat(candle.takerBuyQuoteVolume).isEqualByComparingTo("50.14995");
        assertThat(candle.count).isEqualTo(4);
        assertThat(candle.firstTradeId).isEqualTo(10);
        assertThat(candle.lastTradeId).isEqualTo(13);
    }

    @Test
    void arrivalOrderDoesNotMatter() {
        CandleAccumulator expected = build(minute());
        Random random = new Random(7);
        for (int round = 0; round < 20; round++) {
            List<TradeEvent> shuffled = new ArrayList<>(minute());
            Collections.shuffle(shuffled, random);
            assertThat(build(shuffled)).usingRecursiveComparison().isEqualTo(expected);
        }
    }

    @Test
    void mergingTwoHalvesEqualsBuildingAtOnce() {
        List<TradeEvent> trades = minute();
        CandleAccumulator merged =
                AGGREGATE.merge(build(trades.subList(2, 4)), build(trades.subList(0, 2)));
        assertThat(merged).usingRecursiveComparison().isEqualTo(build(trades));
    }
}
