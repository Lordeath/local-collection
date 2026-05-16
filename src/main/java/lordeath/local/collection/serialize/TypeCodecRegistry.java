package lordeath.local.collection.serialize;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 类型序列化器注册中心。
 */
public final class TypeCodecRegistry {

    private static final List<TypeCodec> CODECS = new CopyOnWriteArrayList<>();

    private TypeCodecRegistry() {
    }

    /**
     * 注册类型编解码器。
     *
     * @param codec 编解码器
     */
    public static void register(TypeCodec codec) {
        if (codec == null) {
            throw new IllegalArgumentException("codec is null");
        }
        CODECS.add(codec);
    }

    /**
     * 按 javaType 查找匹配的编解码器。
     *
     * @param javaType java 类型
     * @return 匹配的编解码器；不存在返回 null
     */
    public static TypeCodec resolve(Class<?> javaType) {
        if (javaType == null) {
            return null;
        }
        for (TypeCodec codec : CODECS) {
            if (codec.supports(javaType)) {
                return codec;
            }
        }
        return null;
    }

    /**
     * 获取该类型建议的数据库字段类型。
     *
     * @param javaType java 类型
     * @return SQL 类型
     */
    public static String resolveSqlType(Class<?> javaType) {
        TypeCodec codec = resolve(javaType);
        return codec == null ? null : codec.getDbType();
    }

    /**
     * 清理测试中的动态注册信息。
     */
    public static void clear() {
        CODECS.clear();
    }
}
