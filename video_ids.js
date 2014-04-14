var _ = require('underscore');
var ClapiClient = require('../lib/imports/transcripts/clapiclient.js');
var MongooseCourse = require('sub').models.dataModels.course.model;
var ThreePlayClient = require('../lib/imports/transcripts/threeplayclient.js');
var async = require('async');
var natural = require('natural');

var courseFields = 'title segments.title segments.segment_id';

var fileIds = {};

var VideoIdFinder = function() {
  this._table = null;
  this._batchesById = null;
  this._threePlayClient = null;
  this._howManySkipped = 0;
  this._howManyFound = 0;
  this._clapiClient = null;
  this._duplicates = 0;
};

_.extend(VideoIdFinder.prototype, {
  computeVideoIds : function(callback) {
    var self = this;
    self._threePlayClient = new ThreePlayClient();
    self._clapiClient = new ClapiClient();

    self._table = [];
    self._getBatchesById(function(err, batchesById) {
      if (err) {
        console.warn("There was an error getting the batches by id...\n", err);
        process.exit(1);
      }
      self._batchesById = batchesById;
      self._iterateFiles(self._computeVideoIdForFile.bind(self), function() {
        callback(self._table);
      });
      
    });
  },

  _getBatchesById : function(callback) {
    var self = this;
    
    self._batchesById = {};
    self._threePlayClient.getBatches(function(err, batches) {
      if (err) {
        callback(err);
      }
      _.each(batches, function(batch) {
        self._batchesById[batch.id] = batch;
      });
      callback(null, self._batchesById);
    });
  },

  _iterateFiles : function(processor, callback) {
    var self = this;

    self._threePlayClient.getPageOfAllFiles(function(err, pageOfFiles) {
      if (err) {
        console.warn("Error getting page of files...\n", err);
        process.exit(1);
      }
      if (!pageOfFiles.length) {
        return callback();
      }
      async.eachSeries(pageOfFiles, processor, function(err) {
        if (err) {
          console.warn("There was an error processing one of the files.");
          process.exit(1);
        }
        self._iterateFiles(processor, callback);
      });
      self._iterateFiles(processor, callback);
    });
  },

  _computeVideoIdForFile : function(fileData, callback) {
    var self = this;
    var batchName = self._batchesById[fileData.batch_id].name;
    var closestSegmentInfo = null;

    self._guessCourse(batchName, fileData, function(err, course, segmentId) {
      if (err) {
        console.warn("Error getting course for (filename, batchName):");
        console.warn("( " +  fileData + ", " + batchName + " )");
        process.exit(1);
      }
      if (!course) {
        console.warn("Failure: " + "( " +  fileData + ", " + batchName + " )");
        console.warn("Unable to find corresponding course.");
        self._howManySkipped += 1;
        return callback();
      }
      console.warn("Found a course for file:\n", fileData.name, "\n");
      closestSegmentInfo = self._getClosestSegmentInfo(course, fileData.name, segmentId);
      if (!fileIds[fileData.id] && closestSegmentInfo) {
        self._table.push({
          filename     : fileData.name,
          folderName   : batchName,
          segmentTitle : closestSegmentInfo.title,
          file_id      : fileData.id,
          video_id     : closestSegmentInfo.segment_id,
          distance     : closestSegmentInfo.distance
        });
        fileIds[fileData.id] = true;
        self._howManyFound += 1;
      } else {
        self._duplicates += 1;
      }
      callback();
    });
  },

  _guessCourse : function(batchName, fileData, callback) {
    var self   = this;

    self._findCourseByFileAttributes(fileData, function(err, course, segmentId) {
      if (err) {
        return callback(err);
      }
      if (course) {
        return callback(err, course, segmentId);
      }
      self._findCourseByBatchName(batchName, callback);
    });
  },
  
  _findCourseByBatchName : function(batchName, callback) {
    MongooseCourse.find({
      title : new RegExp(batchName, 'i')
    }, courseFields, function(err, courses) {
      if (err) {
        return callback(err);
      }
      if (courses && courses.length === 1) {
        return callback(null, courses[0]);
      }
    });
  },
  
  _findCourseByFileAttributes : function(fileData, callback) {
    var self = this;

    self._clapiClient.getFileAttributes({
      filename : fileData.name
    }, function(err, fileAttributes) {
      if (err) {
        return callback(err);
      }
      if (fileAttributes.length > 1) {
        console.warn("Too many courses appear to use filename : " + fileData.name);
        return callback(null, null);
      }
      if (fileAttributes.length === 0) {
        console.warn("No courses appear to use filename : " + fileData.name);
        return callback(null, null);
      }
      MongooseCourse.find({
        nid : fileAttributes[0].course
      }, courseFields, function(err, courses) {
        if (err) {
          return callback(err);
        }
        callback(null, courses[0].toObject(), fileAttributes[0].segment);
      });
    });
  },
    
  _getClosestSegmentInfo : function(course, filename, segmentId) {
    var self = this;
    var weightedSegments = null;

    if (segmentId) {
      return self._getCourseSegmentById(course, segmentId);
    }
    weightedSegments = _.map(_.flatten(course.segments), function(segmentInfo) {
      return _.extend(segmentInfo, {
        distance : natural.LevenshteinDistance(filename, segmentInfo.title, {
          insertion_cost    : 0,
          deletion_cost     : 1,
          substitution_cost : 1
        })
      });
    });
    weightedSegments.sort(function(a, b) {
      return a.distance - b.distance;
    });
    return weightedSegments[0];
  },

  _getCourseSegmentById : function(course, segmentId) {
    var closestSegmentInfo = null;

    _.each(course.segments, function(dayOfSegments) {
      _.each(dayOfSegments, function(segment) {
        if (segmentId === segment.segment_id) {
          closestSegmentInfo = _.extend(segment, {
            distance : 0
          });
        }
      });
    });
    return closestSegmentInfo;
  }
});

var videoIdFinder = new VideoIdFinder({});

videoIdFinder.computeVideoIds(function(rawTable) {
  var table = [];
  var csvTable = [
    [ 'file_id', 'video_id' ]
  ];
  var howManyIndeterminate = 0;
  var indeterminates = [];

  _.each(rawTable, function(row) {
    if (row.distance <=2) {
      table.push(row);
    } else {
      howManyIndeterminate += 1;
      indeterminates.push(row);
    }
  });
  _.each(table, function(row) {
    csvTable.push([ row.file_id, row.video_id ]);
  });
  console.warn("Done computing video ids, the table is:\n");
  require('csv-stringify')(csvTable, function(err, output) {
    console.log(output);
    console.warn(table);
    console.warn("Found: " + videoIdFinder._howManyFound, "\n");
    console.warn("Skipped: " + videoIdFinder._howManySkipped);
    console.warn("Duplicates: " + videoIdFinder._duplicates);
    console.warn("Indeterminate: " + howManyIndeterminate);
    console.warn(indeterminates);
    process.exit(0);
  });
});
