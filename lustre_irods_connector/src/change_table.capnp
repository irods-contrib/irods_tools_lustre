@0x91aad5f4a36030a9;

struct ChangeDescriptor {
  fidstr @0 :Text;
  parentFidstr @1 :Text;
  objectName @2 :Text;
  lustrePath @3 :Text;
  eventType @4 :EventTypeEnum;
  timestamp @5 :Int64;
  operComplete @6 :Bool;
  objectType @7 :ObjectTypeEnum;
  fileSize @8 :Int64;
  crIndex @9 :Int64;

  enum EventTypeEnum {
    other @0;
    create @1;
    unlink @2;
    rmdir @3;
    mkdir @4;
    rename @5;
    writeFid @6;
  }

  enum ObjectTypeEnum {
    file @0;
    dir @1;
  }

}

struct RegisterMapEntry {
  lustrePath @0 :Text;
  irodsRegisterPath @1 :Text;
}

struct ChangeMap {
  entries @0 :List(ChangeDescriptor);
  registerMap @1 :List(RegisterMapEntry);
  resourceId @2 :Int64;
  updateStatus @3 :Text;
  irodsApiUpdateType @4 :Text;
  resourceName @5 :Text;
  maximumRecordsPerSqlCommand @6 :Int64;
}


