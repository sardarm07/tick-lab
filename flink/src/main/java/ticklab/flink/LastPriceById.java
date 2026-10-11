package ticklab.flink;

import java.math.BigDecimal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * {@code LAST_BY_ID(price, trade_id)}: the price of the trade with the highest ID in a group.
 *
 * <p>SQL's {@code LAST_VALUE} means the last row to arrive. Trades can arrive out of order and
 * many share a millisecond, so the last trade is the one with the highest ID (rule 2 of the
 * plan), whatever the arrival order. That takes a user-defined aggregate function.
 */
@FunctionHint(
        input = {@DataTypeHint("DECIMAL(38, 18)"), @DataTypeHint("BIGINT")},
        output = @DataTypeHint("DECIMAL(38, 18)"))
public class LastPriceById extends AggregateFunction<BigDecimal, LastPriceById.Last> {

    /** The accumulator: the highest ID so far and its price. */
    public static class Last {
        public long id = Long.MIN_VALUE;
        public @DataTypeHint("DECIMAL(38, 18)") BigDecimal price;
    }

    @Override
    public Last createAccumulator() {
        return new Last();
    }

    public void accumulate(Last last, BigDecimal price, Long id) {
        if (price != null && id != null && id > last.id) {
            last.id = id;
            last.price = price;
        }
    }

    /** Combines partial results, for when the planner aggregates in two phases. */
    public void merge(Last last, Iterable<Last> others) {
        for (Last other : others) {
            if (other.price != null && other.id > last.id) {
                last.id = other.id;
                last.price = other.price;
            }
        }
    }

    @Override
    public BigDecimal getValue(Last last) {
        return last.price;
    }
}
