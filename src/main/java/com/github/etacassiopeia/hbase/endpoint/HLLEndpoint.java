package com.github.etacassiopeia.hbase.endpoint;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.github.etacassiopeia.hbase.endpoint.service.ExtendedServices;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <h1>HLLEndpoint</h1>
 * The HLLEndpoint class
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 3/08/16
 */
public class HLLEndpoint extends ExtendedServices.HLLService implements Coprocessor, CoprocessorService {

    private static final Log LOG = LogFactory.getLog(HLLEndpoint.class);

    private RegionCoprocessorEnvironment env;
    private Region region;
    private ExtensionRegistry registry;
    private RegionLocator regionLocator;
    private HTable hTable;
    private Connection connection;

    private ExtendedServices.MultiCardinalityResponse.Cardinality process(final ExtendedServices.BaseOfferRequest request) {

        long result = -1;

        byte[] row = request.getRow().toByteArray();
        byte[] family = request.getFamily().toByteArray();
        byte[] qualifier = request.getColumn().toByteArray();

        final Get get = new Get(row);
        get.addColumn(family, qualifier);
        try {
            get.setMaxVersions(1);

            HyperLogLog hll;

            final Result rowResult = region.get(get);
            if (rowResult == null || rowResult.isEmpty()) {
                //we do not have a such row
                if (request.getType().equals(ExtendedServices.BaseOfferRequest.Type.OfferString)) {

                    ExtendedServices.OfferStringRequest offerStringRequest = request.getExtension(ExtendedServices.OfferStringRequest.request);

                    //TODO make decision about constructor parameter
                    hll = HyperLogLog.Builder.withLog2m(8).build();
                    hll.offer(offerStringRequest.getValue());
                } else {
                    ExtendedServices.OfferHLLRequest offerHLLRequest = request.getExtension(ExtendedServices.OfferHLLRequest.request);

                    hll = HyperLogLog.Builder.build(offerHLLRequest.getHll().toByteArray());
                }
                result = hll.cardinality();

                final Put put = new Put(row);
                put.addColumn(family, qualifier, hll.getBytes());
                region.put(put);

            } else {
                final byte[] value = rowResult.value();
                hll = HyperLogLog.Builder.build(value);
                final long oldResult = hll.cardinality();

                if (request.getType().equals(ExtendedServices.BaseOfferRequest.Type.OfferString)) {
                    ExtendedServices.OfferStringRequest offerStringRequest = request.getExtension(ExtendedServices.OfferStringRequest.request);
                    hll.offer(offerStringRequest.getValue());
                } else {
                    try {
                        ExtendedServices.OfferHLLRequest offerHLLRequest = request.getExtension(ExtendedServices.OfferHLLRequest.request);
                        hll.addAll(HyperLogLog.Builder.build(offerHLLRequest.getHll().toByteArray()));
                    } catch (CardinalityMergeException e) {
                        //I`m not sure what I must do with exceptions!
                        throw new IOException(e);
                    }
                }

                result = hll.cardinality();

                if (result != oldResult) {
                    //HLL has been changed
                    final Put put = new Put(row);
                    put.addColumn(family, qualifier, hll.getBytes());
                    region.put(put);
                }
            }
        } catch (WrongRegionException | NotServingRegionException e) {

            //TODO I`m not sure that this is the best solution
//            try {
//                return (ExtendedServices.MultiCardinalityResponse.Cardinality) region.execService(null,
//                        ClientProtos.CoprocessorServiceCall.newBuilder()
//                                .setServiceName(ExtendedServices.HLLService.getDescriptor().getFullName())
//                                .setRow(ByteString.copyFrom(row))
//                                .setMethodName("offerRequest")
//                                .setRequest(request.toByteString()).build()
//                );
//            } catch (IOException e1) {
//                LOG.info(">> " + ExtendedServices.HLLService.getDescriptor().getFullName());
//                LOG.info(">> " + ExtendedServices.HLLService.getDescriptor().findMethodByName("offerRequest").getFullName());
//                LOG.error(e.getMessage(), e);
//            }

            try {
                LOG.info("the "
                        + (e instanceof NotServingRegionException ? "NotServingRegionException" : "WrongRegionException")
                        + " exception has been occurred from region "
                        + region.getRegionInfo().getRegionId()
                        + " for key " + Bytes.toString(row)
                        + " that is located in region : " + regionLocator.getRegionLocation(row, true).getRegionInfo().getEncodedName()
                );

                hTable.clearRegionCache();

                hTable.coprocessorService(
                        ExtendedServices.HLLService.class,
                        row,
                        row,
                        new Batch.Call<ExtendedServices.HLLService, ExtendedServices.MultiCardinalityResponse.Cardinality>() {
                            public ExtendedServices.MultiCardinalityResponse.Cardinality call(ExtendedServices.HLLService service)
                                    throws IOException {
                                BlockingRpcCallback<ExtendedServices.MultiCardinalityResponse.Cardinality>
                                        rpcCallback =
                                        new BlockingRpcCallback<>();

                                service.offerRequest(null, request, rpcCallback);

                                return rpcCallback.get();
                            }
                        }
                );
            } catch (Throwable throwable) {
                LOG.error(e.getMessage(), e);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return ExtendedServices.MultiCardinalityResponse.Cardinality.newBuilder()
                .setRow(request.getRow())
                .setFamily(request.getFamily())
                .setColumn(request.getColumn())
                .setCardinality(result)
                .build();
    }


    @Override
    public void batchOfferRequest(RpcController controller, ExtendedServices.OfferRequests request, RpcCallback<ExtendedServices.MultiCardinalityResponse> done) {
        List<ExtendedServices.MultiCardinalityResponse.Cardinality> responseList = new ArrayList<>(request.getRequestsCount());
        for (ExtendedServices.BaseOfferRequest r : request.getRequestsList()) {
            try {
                responseList.add(process(ExtendedServices.BaseOfferRequest.parseFrom(r.toByteArray(), registry)));
            } catch (InvalidProtocolBufferException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        done.run(ExtendedServices.MultiCardinalityResponse.newBuilder().addAllCardinalities(responseList).build());
    }

    @Override
    public void offerRequest(RpcController controller, ExtendedServices.BaseOfferRequest request, RpcCallback<ExtendedServices.MultiCardinalityResponse.Cardinality> done) {
        try {
            LOG.info("offerRequest method has been called from region "
                    + region.getRegionInfo().getRegionId()
                    + " for key " + Bytes.toString(request.getRow().toByteArray())
                    + " that is located in region : " + regionLocator.getRegionLocation(request.getRow().toByteArray(), true).getRegionInfo().getEncodedName()
            );
            //Protocol buffers extensions getting lost on the way?
            done.run(process(ExtendedServices.BaseOfferRequest.parseFrom(request.toByteArray(), registry)));
        } catch (InvalidProtocolBufferException e) {
            LOG.error(e.getMessage(), e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (coprocessorEnvironment instanceof RegionCoprocessorEnvironment) {
            env = (RegionCoprocessorEnvironment) coprocessorEnvironment;
            region = env.getRegion();

            registry = ExtensionRegistry.newInstance(); // create extension registry
            registry.add(ExtendedServices.OfferStringRequest.request);
            registry.add(ExtendedServices.OfferHLLRequest.request);
            ExtendedServices.registerAllExtensions(registry);

            connection = ConnectionFactory.createConnection();
            TableName tableName = region.getTableDesc().getTableName();
            hTable = (HTable) connection.getTable(tableName);
            regionLocator = connection.getRegionLocator(tableName);
        } else
            throw new CoprocessorException("Must be loaded on a table region!");
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        hTable.close();
        connection.close();
    }

    @Override
    public Service getService() {
        return this;
    }
}