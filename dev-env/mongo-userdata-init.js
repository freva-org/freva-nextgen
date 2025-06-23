db = db.getSiblingDB('search_stats');

function multiValuedStringField(description) {
    return {
        oneOf: [
        { bsonType: "array", items: { bsonType: "string" } },
        { bsonType: "string" }
        ],
        description: description
    };
}

db.createCollection("user_data", {
  validator: {
    $jsonSchema: {
        bsonType: "object",
        required: ["file", "uri"],
        additionalProperties: false,
        properties: {
          "_id": {},
          // Unique Identifier Fields
          "file": {
            bsonType: "string",
            description: "Must be a string and is required."
          },
          "uri": {
            bsonType: "string",
            description: "Must be a string and is required."
          },
          // Path Field for Re-crawling
          "path": {
            bsonType: "string",
            description: "Path inputed by user re-crawling purpose."
          },
          // Versioning Fields
          "_version_": {
            bsonType: "long",
            description: "Version number as a long integer."
          },
          "version": {
            bsonType: "string",
            description: "Version string."
          },
    
          // Timestamp Fields
          "timestamp": {
            bsonType: "double",
            description: "Timestamp as a double."
          },
          "creation_time": {
            bsonType: "date",
            description: "Creation time as a date."
          },
    
          // File Information Fields
          "file_name": {
            bsonType: "string",
            description: "Name of the file."
          },
          "file_no_version": {
            bsonType: "string",
            description: "File name without version."
          },
          "bbox": {
            bsonType: "array",
            minItems: 4,
            maxItems: 4,
            items: {
              bsonType: "double"
            },
            description: "Bounding box coordinates [west, south, east, north] in degrees"
          },
          // Standard Facet Fields (Multivalued)
          "cmor_table": multiValuedStringField("Array of CMOR tables or a single string."),
          "experiment": multiValuedStringField("Array of experiments or a single string."),
          "ensemble": multiValuedStringField("Array of ensembles or a single string."),
          "fs_type": {
            bsonType: "string",
            description: "File system type."
          },
          "grid_label": multiValuedStringField("Array of grid labels or a single string."),
          "institute": multiValuedStringField("Array of institutes or a single string."),
          "model": multiValuedStringField("Array of models or a single string."),
          "project": multiValuedStringField("Array of projects or a single string."),
          "product": multiValuedStringField("Array of products or a single string."),
          "realm": multiValuedStringField("Array of realms or a single string."),
          "variable": multiValuedStringField("Array of variables or a single string."),
          "time": {
            bsonType: "string",
            pattern: "^\\[\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z TO \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z\\]$",
            description: "Time range in ISO 8601 format"
          },
          "time_aggregation": multiValuedStringField("Array of time aggregations or a single string."),
          "time_frequency": multiValuedStringField("Array of time frequencies or a single string."),
    
          // Extra Facet Fields
          "dataset": {
            bsonType: "string",
            description: "Dataset name."
          },
          "driving_model": multiValuedStringField("Array of driving models or a single string."),
          "format": {
            bsonType: "string",
            description: "File format."
          },
          "grid_id": multiValuedStringField("Array of grid IDs or a single string."),
          "level_type": multiValuedStringField("Array of level types or a single string."),
          "rcm_name": multiValuedStringField("Array of RCM names or a single string."),
          "rcm_version": multiValuedStringField("Array of RCM versions or a single string."),
          "user": {
            bsonType: "string",
            description: "User associated with the data."
          },
    
          // Future Dataset Fields
          "future": {
            bsonType: "string",
            description: "Future dataset information."
          },
          "future_id": {
            bsonType: "string",
            description: "Future dataset ID."
          }
        }
      }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.user_data.createIndex({ file: 1 }, { unique: true });
db.user_data.createIndex({ uri: 1 }, { unique: true });
