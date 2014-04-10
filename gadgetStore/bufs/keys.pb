package gadgetStore;

message Gadget {
  required int64 offset = 1;
  required int64 size = 2;
}

message Key {
  required string key = 1;
  required Gadget gadget = 2;
}

message KeyList {
  repeated Key keys = 1;
}

