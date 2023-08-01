package org.apache.accumulo.core.file.rfile.rfiletests.keycreator;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.rfiletests.FieldInjector;
import org.apache.accumulo.core.file.rfile.uids.UID;
import org.apache.accumulo.core.file.rfile.uids.Uid;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public class UidValueBitstreamShard extends GlobalIndexKeyCreator{
    public UidValueBitstreamShard(CreatorConfiguration config, RFile.Writer writer) {
        super(config, writer);
    }

    @Override
    protected Collection<KeyValue> formKeyPart(String datatype, Text fv, String fieldName, Collection<UID> uids, Text cv, String myShard) {
        var bt = shardToBitstream_v0(myShard,5);
        Text cq = new Text(datatype + NULL);
        cq.append(bt,0,bt.length);

        var uidBuilder = Uid.List.newBuilder();
        //var mapping  = mymapping.get(cv);

        var docsInfv = config.fieldValueToDoc.get(fv);

        //var intersection = mapping.stream().filter(docsInfv::contains).collect(Collectors.toList());

        docsInfv.forEach(x -> uidBuilder.addUID(x.toString()));
        uidBuilder.setCOUNT(docsInfv.size());
        uidBuilder.setIGNORE(false);
        var key = new Key(fv, new Text(fieldName), cq, cv);
        return Collections.singleton(new KeyValue(key, new Value(uidBuilder.build().toByteArray())));

    }
}
