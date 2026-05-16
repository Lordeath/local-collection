package lordeath.local.collection.serialize;

/**
 * 类型序列化编解码器。
 * <p>
 * 当持久化类型不在内置白名单时，可以通过该接口在数据库读写时接管序列化和反序列化。
 */
public interface TypeCodec {

    /**
     * 是否支持指定 Java 类型。
     *
     * @param javaType java 类型
     * @return 是否支持
     */
    boolean supports(Class<?> javaType);

    /**
     * 序列化为可落库的字符串。
     *
     * @param value 对象值
     * @return 序列化后的字符串
     */
    String serialize(Object value);

    /**
     * 反序列化从数据库读出的字符串。
     *
     * @param rawString 数据库存储字符串
     * @param targetType 目标类型
     * @return 反序列化后的对象
     */
    Object deserialize(String rawString, Class<?> targetType);

    /**
     * 该类型建议使用的 SQL 列类型。
     *
     * @return SQL 类型
     */
    String getDbType();
}
