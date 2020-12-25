package sao.so.hive.util.udaf;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

/**
 * @author huisx
 * @description gorup后分组存储到roaringbitmap里面
 * @date 2020年12月22日
 **/
@Description(name = "count_bit_map_distinct",
        value = "_FUNC_(x) - Returns  the element distinct count ")
public class RoaringBitMapCountUDAF extends AbstractGenericUDAFResolver {
    private static final Log LOG = LogFactory.getLog(RoaringBitMapCountUDAF.class.getName());

    /**
     * 验证传入函数中的参数
     * @param params sql中函数里面的参数
     * @return 聚合函数逻辑体
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] params) throws SemanticException {
        if (params.length > 1) {
            throw new UDFArgumentLengthException("Exactly one argument is expected.");
        }
        return new BitmapDistinctUDAFEvaluator();
    }

    /**
     * 聚合函数实现类
     */
    public static class BitmapDistinctUDAFEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector intInputOI;

        private BinaryObjectInspector partialBufferOI;

        /**
         *
         * @param mode
         * PARTIAL1: 从原始数据到部分聚合数据的过程，会调用iterate()和terminatePartial()
         * 		可以理解为MapReduce过程中的map阶段
         *
         * PARTIAL2: 从部分聚合数据到部分聚合数据的过程（多次聚合），会调用merge()和terminatePartial()
         * 		可以理解为MapReduce过程中的combine阶段
         *
         * FINAL: 从部分聚合数据到全部聚合数据的过程，会调用merge()和 terminate()
         * 		可以理解为MapReduce过程中的reduce阶段
         *
         * COMPLETE: 从原始数据直接到全部聚合数据的过程，会调用iterate()和terminate()
         * 		可以理解为MapReduce过程中的直接map输出阶段，没有reduce阶段
         * @param parameters
         */
        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                this.intInputOI = (PrimitiveObjectInspector) parameters[0];
                LOG.debug("-----------------------> input data class :"+intInputOI.getPrimitiveCategory());
            } else {
                this.partialBufferOI = (BinaryObjectInspector) parameters[0];
            }
            if (mode == Mode.FINAL) {
                return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
            }
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;

        }

        /**
         * 创建计算过程中数据存储的缓存区
         * @return bitmap缓存区
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            BitMapBuffer bitMapAgg = new BitMapBuffer();
            reset(bitMapAgg);
            return bitMapAgg;
        }

        /**
         * 清空缓存区
         * @param aggregationBuffer 每个计算节点的缓存区
         */
        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            BitMapBuffer bitMapAgg = (BitMapBuffer) aggregationBuffer;
            bitMapAgg.reset();
        }

        /**
         * Map端读取数据后存入缓存区
         * @param agg 计算节点的缓存区
         * @param parameters 加载过来的数据
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null || parameters.length != 1) {
                return;
            }
            if (parameters[0] != null) {
                BitMapBuffer myagg = (BitMapBuffer) agg;
                myagg.addItem(PrimitiveObjectInspectorUtils.getInt(parameters[0], intInputOI));
            }


        }

        /**
         * 序列化缓冲区的数据用于计算节点交换计算
         * @param agg 每个计算节点的缓存区
         * @return 序列化后的二进制流对象
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LOG.debug("terminatePartial");
            BitMapBuffer myagg = (BitMapBuffer) agg;
            try {
                return myagg.getPartial();
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        /**
         * reduce阶段合并计算节点的数据
         * @param agg 当前计算节点的缓存区
         * @param partial 部分节点序号化后的数据
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                BitMapBuffer myagg = (BitMapBuffer) agg;
                byte[] partialBuffer = this.partialBufferOI.getPrimitiveJavaObject(partial);
                try {
                    myagg.merge(partialBuffer);
                } catch (IOException e) {
                    throw new HiveException(e);
                }
            }
        }

        /**
         * 计算得出最后的结果
         * @param agg 每个计算节点的缓存区
         * @return 聚合结果值
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            try {
                BitMapBuffer myagg = (BitMapBuffer) agg;
                return myagg.getCardinalityCount();
            } catch (Exception e) {
                throw new HiveException(e);
            }
        }
    }


}
