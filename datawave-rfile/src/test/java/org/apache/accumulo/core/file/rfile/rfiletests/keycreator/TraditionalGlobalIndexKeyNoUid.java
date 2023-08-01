package org.apache.accumulo.core.file.rfile.rfiletests.keycreator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.uids.UID;
import org.apache.accumulo.core.file.rfile.uids.Uid;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.Collections;

public class TraditionalGlobalIndexKeyNoUid extends GlobalIndexKeyCreator {
    public TraditionalGlobalIndexKeyNoUid(CreatorConfiguration config, RFile.Writer writer) {
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
        return Collections.singleton(new KeyValue(key,EMPTY_VALUE));
    }
}
