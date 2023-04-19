/* eslint-disable require-await */
// import this node module via npm. This wraps the native browser SpeechRecognition API.
// import SpeechToText from 'speech-to-text'


class STTSpeechAPI {
    constructor (language = 'en-US') {
      this.stt = new SpeechToText(this.onFinalResult, this.onStop, this.onInterimResult, language)
    }
  
    startListening () {
      this.stt.startListening()
    }
  
    stopListening () {
      this.stt.stopListening()
    }
  
    abort () {
      this.stt.recognition.abort()
      this.stt.stopListening()
    }
  
    onFinalResult (text) {
      console.log("FINAL: " + text)
      const m = {
        text,
        final: true,
      }
      window.sap.cai.webclient.onSTTResult(m)
    }
  
    onInterimResult (text) {
      const m = {
        text,
        final: false,
      }
      window.sap.cai.webclient.onSTTResult(m)
      console.log("INTERIM: " + text)
    }
  
    onStop () {
      console.log("END")
      const m = {
        text: '',
        final: true,
      }
      window.sap.cai.webclient.onSTTResult(m)
    }
  }
  
  let stt = null
  const webclientBridge = {
    sttGetConfig: async () => {
      return {
        useMediaRecorder: false,
      }
    },
  
    sttStartListening: async (params) => {
      console.log("HERE")
      const [metadata] = params
      const { language, _ } = metadata
      stt = new STTSpeechAPI(language)
      stt.startListening()
    },
  
    sttStopListening: () => {
      stt.stopListening()
    },
  
    sttAbort: () => {
      stt.abort()
    },
  }
  
  window.sapcai = {
      webclientBridge,
  }