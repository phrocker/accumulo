package org.apache.accumulo.core.file.rfile.rfiletests.keycreator;

import org.apache.accumulo.core.file.rfile.RFile;

public class KeyCreatorBuilder
{
    private final RFile.Writer writer;
    private final CreatorConfiguration config;
    private GlobalIndexStrategy strategy;

    private KeyCreatorBuilder(CreatorConfiguration config, RFile.Writer writer){
        this.config=config;
        this.writer = writer;
    }

    public static KeyCreatorBuilder newBuilder(CreatorConfiguration config, RFile.Writer writer){
        return new KeyCreatorBuilder(config, writer);
    }

    public KeyCreatorBuilder withStrategy(GlobalIndexStrategy strat){
        this.strategy = strat;
        return this;
    }

    public GlobalIndexKeyCreator build(){
        switch (strategy.strat){
            case ORIGINAL:
                return new TraditionalGlobalIndexKey(config, writer);
                /*
            case ORIGINAL_NOUIDS:
                return new TraditionalGlobalIndexKeyNoUid(config, writer);
            case BITSTREAMSHARD:
                return new UidValueBitstreamShard(config, writer);
            case UIDATEND_REGULARSHARD:
                return new UidEndStringShard(config, writer);
            case UIDATEND_BITSTREAMSHARDINROW:
                return new UidCqBitstreamShard(config, writer);
            default:
                throw new IllegalStateException("Unexpected value: " + strategy);

                 */
            default:
                return null;
        }
    }

}
