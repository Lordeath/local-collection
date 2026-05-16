package lordeath.local.collection.db.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlDialectUtilTest {

    @Test
    void buildGroupByKeyExpressionForSingleColumnShouldReturnPassthroughExpression() {
        assertEquals("name AS key_col", SqlDialectUtil.buildGroupByKeyExpression(
                Arrays.asList("name"), "key_col", "sqlite"));
        assertEquals("name AS key_col", SqlDialectUtil.buildGroupByKeyExpression(
                Arrays.asList("name"), "key_col", "h2"));
    }

    @Test
    void buildGroupByKeyExpressionForMultiColumnShouldUseSqliteConcatStyleByDefault() {
        assertEquals("name || '.' || age AS key_col", SqlDialectUtil.buildGroupByKeyExpression(
                Arrays.asList("name", "age"), "key_col", "sqlite"));
        assertEquals("name || '.' || age AS key_col", SqlDialectUtil.buildGroupByKeyExpression(
                Arrays.asList("name", "age"), "key_col", "unknown"));
    }

    @Test
    void buildGroupByKeyExpressionForMultiColumnShouldUseH2ConcatStyle() {
        assertEquals("CONCAT(name, '.', age, '.', city) AS key_col", SqlDialectUtil.buildGroupByKeyExpression(
                Arrays.asList("name", "age", "city"), "key_col", "h2"));
    }

    @Test
    void buildGroupByKeyExpressionForEmptyGroupByShouldReturnBlankKeyExpression() {
        assertEquals("'' AS key_col", SqlDialectUtil.buildGroupByKeyExpression(
                Arrays.asList(), "key_col", "sqlite"));
    }
}
