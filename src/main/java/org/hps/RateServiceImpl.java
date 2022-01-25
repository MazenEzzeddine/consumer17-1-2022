package org.hps;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RateServiceImpl extends RateServiceGrpc.RateServiceImplBase {

    private static final Logger log = LogManager.getLogger(RateServiceImpl.class);

    @Override
    public void consumptionRate(Empty request, StreamObserver<RateResponse> responseObserver) {
        log.info("received new rate request");
        RateResponse rate = RateResponse.newBuilder()
                .setRate(ConsumerThread.maxConsumptionRatePerConsumer)
                        .build();

        responseObserver.onNext(rate);
        responseObserver.onCompleted();

    }
}
