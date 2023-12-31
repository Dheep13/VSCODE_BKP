//"use strict";

//Object.defineProperty(exports, "__esModule", {
//  value: true
//});
//exports.default = void 0;

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

var SpeechToText =
/*#__PURE__*/
function () {
  /*
  This module is largely inspired by this article:
  https://developers.google.com/web/updates/2013/01/Voice-Driven-Web-Apps-Introduction-to-the-Web-Speech-API
  
  Arguments for the constructor:
     - onFinalised - a callback that will be passed the finalised transcription from the cloud. Slow, but accuate.
    - onEndEvent - a callback that will be called when the end event is fired (speech recognition engine disconnects).
    - onAnythingSaid - a callback that will be passed interim transcriptions. Fairly immediate, but less accurate than finalised text.
    - language - the language to interpret against. Default is US English.
     */
  function SpeechToText(onFinalised, onEndEvent, onAnythingSaid) {
    var _this = this;
		console.log("inside")
    var language = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : 'en-US';

    _classCallCheck(this, SpeechToText);

    // Check to see if this browser supports speech recognition
    // https://developer.mozilla.org/en-US/docs/Web/API/SpeechRecognition#Browser_compatibility
    if (!('webkitSpeechRecognition' in window)) {
      throw new Error("This browser doesn't support speech recognition. Try Google Chrome.");
    }

    var SpeechRecognition = window.webkitSpeechRecognition;
    this.recognition = new SpeechRecognition(); // set interim results to be returned if a callback for it has been passed in

    this.recognition.interimResults = !!onAnythingSaid;
    this.recognition.lang = language;
    var finalTranscript = ''; // process both interim and finalised results

    this.recognition.onresult = function (event) {
				console.log("ON RESULT")
      var interimTranscript = ''; // concatenate all the transcribed pieces together (SpeechRecognitionResult)

      for (var i = event.resultIndex; i < event.results.length; i += 1) {
        var transcriptionPiece = event.results[i][0].transcript; // check for a finalised transciption in the cloud

        if (event.results[i].isFinal) {
          finalTranscript += transcriptionPiece;
          onFinalised(finalTranscript);
          finalTranscript = '';
        } else if (_this.recognition.interimResults) {
          interimTranscript += transcriptionPiece;
          onAnythingSaid(interimTranscript);
        }
      }
    };

    this.recognition.onend = function () {
      onEndEvent();
    };
  }

  _createClass(SpeechToText, [{
    key: "startListening",
    value: function startListening() {
      this.recognition.start();
    }
  }, {
    key: "stopListening",
    value: function stopListening() {
      this.recognition.stop();
    }
  }]);

  return SpeechToText;
}();

//exports.default = SpeechToText;<br>