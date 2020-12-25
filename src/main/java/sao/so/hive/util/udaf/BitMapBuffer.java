package sao.so.hive.util.udaf;


import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author huisx
 * @description bitmap缓存区 自定义函数数据在内存中的缓存区
 * @date 2020年12月21日
 **/

public class BitMapBuffer implements GenericUDAFEvaluator.AggregationBuffer {
    private static final Logger LOG = Logger.getLogger(BitMapBuffer.class);

    private MutableRoaringBitmap bitMap;
    // 初始化bitmap
    public BitMapBuffer(){
        bitMap = new MutableRoaringBitmap();
    }
    // 添加元素
    public void addItem(int id) {
        bitMap.add(id);
    }

    /**
     * 合并各个exectuer的数据 合并过程：
     *  （this） (buffer) (buffer) (buffer)
     *      \      /       /       /
     *        this        /       /
     *          \        /       /
     *             this         /
     *               \         /
     *                   this
     * @param buffer
     *  部分合并结果
     **/
    public void merge(byte[] buffer) throws IOException{
        if (buffer == null) {
            return;
        }

        ImmutableRoaringBitmap other = new ImmutableRoaringBitmap(ByteBuffer.wrap(buffer));
        if (bitMap == null) {
            LOG.debug("bitMap is null; other size = "+other.getLongSizeInBytes()+" count = " + other.getLongCardinality());
            // 赋值
            bitMap = other.toMutableRoaringBitmap();
        } else {
            // 合并
            bitMap.or( other.toMutableRoaringBitmap());
        }
    }

    /**
     * bitmap数据序列化为字节数组，用于reduce过程数据传输
     * @return
     *  bitmap的数据
     */
    public byte[] getPartial() throws IOException {
        if (bitMap == null) {
            return null;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bitMap.serialize(dos);
        dos.close();
        return bos.toByteArray();
    }

    /**
     * 获得bitmap中不同元素的个数
     * @return
     *  个数
     */
    public int  getCardinalityCount(){
        return bitMap.getCardinality();
    }

    public void reset() {
        bitMap.clear();
    }



}
