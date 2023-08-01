package org.apache.accumulo.core.file.rfile.rfiletests.keycreator;

import com.google.common.collect.Multimap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.rfiletests.FieldInjector;
import org.apache.accumulo.core.file.rfile.uids.UID;
import org.apache.accumulo.core.file.rfile.uids.Uid;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public class UidCqBitstreamShard extends GlobalIndexKeyCreator{
    public UidCqBitstreamShard(CreatorConfiguration config, RFile.Writer writer) {
        super(config, writer);
    }

    @Override
    protected Collection<KeyValue> formKeyPart(String datatype, Text fv, String fieldName, Collection<UID> uids, Text cv, String myShard) {
        var bt = shardToBitstream_v0(myShard,5);

        Text row = new Text(fv);
        row.append(bt,0,bt.length);


        var docsInfv = config.fieldValueToDoc.get(fv);

        //var intersection = mapping.stream().filter(docsInfv::contains).collect(Collectors.toList());

        List<KeyValue> kvs = new ArrayList<>();
        for(var docId : docsInfv) {
            Text cq = new Text(datatype + NULL + docId.toString());
            var key = new Key(row, new Text(fieldName), cq, cv);
            kvs.add(new KeyValue(key, EMPTY_VALUE));
        }
        return kvs;

    }
}
