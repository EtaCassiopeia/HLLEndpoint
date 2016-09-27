package com.github.etacassiopeia.hbase.endpoint;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.github.etacassiopeia.hbase.endpoint.service.ExtendedServices;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <h1>HLLWriter</h1>
 * The HLLWriter class
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 4/08/16
 */
public class HLLWriter {

    private static final Log LOG = LogFactory.getLog(HLLWriter.class);

    private final HTable table;

    private final HashMap<String, List<ExtendedServices.BaseOfferRequest>> cache = new HashMap<>();

    private final AtomicInteger requestCount = new AtomicInteger(0);

    public HLLWriter(HTable table) {
        this.table = table;
    }

    public void send(ExtendedServices.BaseOfferRequest request) {
        try {
            synchronized (cache) {
                String regionId = getRegionId(request.getRow().toByteArray());
                System.out.println(regionId);
                if (cache.containsKey(regionId)) {
                    cache.get(regionId).add(request);
                } else {
                    List<ExtendedServices.BaseOfferRequest> list = new ArrayList<>();
                    list.add(request);
                    cache.put(regionId, list);
                }
                requestCount.incrementAndGet();
                flushIfRequired();
            }
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    //I'm not going to implement a fantastic algorithm for now
    private void flushIfRequired() throws Throwable {
        if (requestCount.get() >= 10) {
            for (Map.Entry<String, List<ExtendedServices.BaseOfferRequest>> entry : cache.entrySet()) {
                ExtendedServices.OfferRequests batchRequest = ExtendedServices.OfferRequests.newBuilder()
                        .addAllRequests(entry.getValue()).build();

                //final HRegionInfo regionInfo = table.getRegionLocator().getRegionLocation(batchRequest.getRequests(0).getRow().toByteArray(), true).getRegionInfo();
                final ExtendedServices.BaseOfferRequest firstRequest = batchRequest.getRequests(0);

                final ExtendedServices.OfferRequests finalBatchRequest = batchRequest;
                table.coprocessorService(
                        ExtendedServices.HLLService.class,
                        firstRequest.getRow().toByteArray(),  // start row key, start key from regionInfo object always is null
                        firstRequest.getRow().toByteArray(), // end row key
                        new Batch.Call<ExtendedServices.HLLService, ExtendedServices.MultiCardinalityResponse>() {
                            public ExtendedServices.MultiCardinalityResponse call(ExtendedServices.HLLService service)
                                    throws IOException {
                                BlockingRpcCallback<ExtendedServices.MultiCardinalityResponse>
                                        rpcCallback =
                                        new BlockingRpcCallback<>();

                                service.batchOfferRequest(null, finalBatchRequest, rpcCallback);

                                //TODO check results
                                ExtendedServices.MultiCardinalityResponse response = rpcCallback.get();
                                for (ExtendedServices.MultiCardinalityResponse.Cardinality r : response.getCardinalitiesList()) {
                                    System.out.println("Cardinality response for " + Bytes.toString(r.getRow().toByteArray()) + " is " + r.getCardinality());
                                }
                                return response;
                            }
                        }
                );
            }
            cache.clear();
            requestCount.set(0);
            table.clearRegionCache();
        }
    }

    private String getRegionId(byte[] rowkey) throws IOException {
        return table.getRegionLocator().getRegionLocation(rowkey).getRegionInfo().getEncodedName();
    }

    public static ExtendedServices.BaseOfferRequest createRequest(byte[] row, byte[] family, byte[] column, String value) {
        ExtendedServices.OfferStringRequest stringOfferRequest = ExtendedServices.OfferStringRequest.newBuilder().setValue(value).build();
        return wrap(row, family, column, ExtendedServices.BaseOfferRequest.Type.OfferString, ExtendedServices.OfferStringRequest.request, stringOfferRequest);
    }

    public static ExtendedServices.BaseOfferRequest createRequest(byte[] row, byte[] family, byte[] column, HyperLogLog hll) throws IOException {
        ExtendedServices.OfferHLLRequest hllOfferRequest = ExtendedServices.OfferHLLRequest.newBuilder().setHll(ByteString.copyFrom(hll.getBytes())).build();
        return wrap(row, family, column, ExtendedServices.BaseOfferRequest.Type.OfferHLL, ExtendedServices.OfferHLLRequest.request, hllOfferRequest);
    }

    private static <Type> ExtendedServices.BaseOfferRequest wrap(byte[] row, byte[] family, byte[] column, ExtendedServices.BaseOfferRequest.Type type, GeneratedMessage.GeneratedExtension<ExtendedServices.BaseOfferRequest, Type> extension, Type request) {
        return ExtendedServices.BaseOfferRequest.newBuilder()
                .setRow(ByteString.copyFrom(row))
                .setFamily(ByteString.copyFrom(family))
                .setColumn(ByteString.copyFrom(column))
                .setType(type)
                .setExtension(extension, request)
                .build();
    }
}
