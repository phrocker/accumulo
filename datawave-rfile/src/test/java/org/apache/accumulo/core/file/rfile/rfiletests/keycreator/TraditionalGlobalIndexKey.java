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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public class TraditionalGlobalIndexKey extends GlobalIndexKeyCreator {
    public TraditionalGlobalIndexKey(CreatorConfiguration config, RFile.Writer writer) {
        super(config, writer);
    }

    @Override
    protected Collection<KeyValue> formKeyPart(String datatype, Text fv, String fieldName, Collection<UID> docsInfv, Text cv, String myShard) {
        var uidBuilder = Uid.List.newBuilder();
        //var mapping  = mymapping.get(cv);

        Text cq = new Text(datatype + NULL + myShard);

        //var intersection = mapping.stream().filter(docsInfv::contains).collect(Collectors.toList());

        docsInfv.forEach(x -> uidBuilder.addUID(x.toString()));
        uidBuilder.setCOUNT(docsInfv.size());
        uidBuilder.setIGNORE(false);
        var key = new Key(fv, new Text(fieldName), cq, cv);
        return Collections.singleton(new KeyValue(key,new Value(uidBuilder.build().toByteArray())));
    }
}
