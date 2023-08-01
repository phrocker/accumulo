package org.apache.accumulo.core.file.rfile.rfiletests.keycreator;

import java.io.IOException;

public class GlobalIndexStrategy implements IndexingStrategy {

    public enum STRATEGY {
        ORIGINAL;
        /*
                BITSTREAMSHARD,
        UIDATEND_REGULARSHARD,
        UIDATEND_BITSTREAMSHARDINROW,
        ORIGINAL_NOUIDS;

         */
//            UIDATEND_STRINGSHARDINROW
        // UIDATEND_BITSTREAMSHARDINCQ,
        //UIDATEND_BITSTREAMSHARDINCQ_DATATYPEINCF

    };

    final STRATEGY strat;

    public GlobalIndexStrategy(STRATEGY strat) {
        this.strat = strat;
    }

    @Override
    public void select(TestRFileGenerator generator) throws IOException {
        var gen = ShardIndexGenerator.class.cast(generator);
       /* switch (strat) {
            case ORIGINAL:
                gen.writeGlobalIndexOld();
                break;
            case BITSTREAMSHARD:
                gen.writeGlobalIndexOld2();
                break;
            case UIDATEND_REGULARSHARD:
                gen.writeGlobalIndexNew();
                break;
            case UIDATEND_BITSTREAMSHARDINROW:
                gen.writeGlobalIndexNewNew();
                break;
//                case UIDATEND_STRINGSHARDINROW:
//                    gen.writeGlobalIndexNewNewNew2();
//                  break;
                /*
            case UIDATEND_BITSTREAMSHARDINCQ:
                gen.writeGlobalIndexNewNew2();
                break;
            case UIDATEND_BITSTREAMSHARDINCQ_DATATYPEINCF:
                gen.writeGlobalIndexNewNew3();
                break;
                */
        }
}
