package extension

var (
	_mimeTypes        map[MIME]string
	_mimeTypesReverse map[string]MIME
)

// MIME is MIME types in number.
// Please see: https://github.com/bsElyes/rsocket/blob/master/Extensions/WellKnownMimeTypes.md
type MIME int8

// All MIMEs
const (
	ApplicationAvro MIME = iota
	ApplicationCBOR
	ApplicationGraphql
	ApplicationGzip
	ApplicationJavascript
	ApplicationJSON
	ApplicationOctetStream
	ApplicationPDF
	ApplicationThrift
	ApplicationProtobuf
	ApplicationXML
	ApplicationZip
	AudioAAC
	AudioMP3
	AudioMP4
	AudioMPEG3
	AudioMPEG
	AudioOGG
	AudioOpus
	AudioVorbis
	ImageBMP
	ImageGIF
	ImageHEICSequence
	ImageHEIC
	ImageHEIFSequence
	ImageHEIF
	ImageJPEG
	ImagePNG
	ImageTIFF
	MultipartMixed
	TextCSS
	TextCSV
	TextHTML
	TextPlain
	TextXML
	VideoH264
	VideoH265
	VideoVP8
	Hessian
	JavaObject
	CloudEventsJSON
	MessageMimeType          MIME = 0x7A
	MessageAcceptMimeTypes   MIME = 0x7B
	MessageAuthentication    MIME = 0x7C
	MessageZipkin            MIME = 0x7D
	MessageRouting           MIME = 0x7E
	MessageCompositeMetadata MIME = 0x7F
)

func init() {
	_mimeTypes = map[MIME]string{
		ApplicationAvro:          "application/avro",
		ApplicationCBOR:          "application/cbor",
		ApplicationGraphql:       "application/graphql",
		ApplicationGzip:          "application/gzip",
		ApplicationJavascript:    "application/javascript",
		ApplicationJSON:          "application/json",
		ApplicationOctetStream:   "application/octet-stream",
		ApplicationPDF:           "application/pdf",
		ApplicationThrift:        "application/vnd.apache.thrift.binary",
		ApplicationProtobuf:      "application/vnd.google.protobuf",
		ApplicationXML:           "application/xml",
		ApplicationZip:           "application/zip",
		AudioAAC:                 "audio/aac",
		AudioMP3:                 "audio/mp3",
		AudioMP4:                 "audio/mp4",
		AudioMPEG3:               "audio/mpeg3",
		AudioMPEG:                "audio/mpeg",
		AudioOGG:                 "audio/ogg",
		AudioOpus:                "audio/opus",
		AudioVorbis:              "audio/vorbis",
		ImageBMP:                 "image/bmp",
		ImageGIF:                 "image/gif",
		ImageHEICSequence:        "image/heic-sequence",
		ImageHEIC:                "image/heic",
		ImageHEIFSequence:        "image/heif-sequence",
		ImageHEIF:                "image/heif",
		ImageJPEG:                "image/jpeg",
		ImagePNG:                 "image/png",
		ImageTIFF:                "image/tiff",
		MultipartMixed:           "multipart/mixed",
		TextCSS:                  "text/css",
		TextCSV:                  "text/csv",
		TextHTML:                 "text/html",
		TextPlain:                "text/plain",
		TextXML:                  "text/xml",
		VideoH264:                "video/H264",
		VideoH265:                "video/H265",
		VideoVP8:                 "video/VP8",
		Hessian:                  "application/x-hessian",
		JavaObject:               "application/x-java-object",
		CloudEventsJSON:          "application/cloudevents+json",
		MessageMimeType:          "message/x.rsocket.mime-type.v0",
		MessageAcceptMimeTypes:   "message/x.rsocket.accept-mime-types.v0",
		MessageAuthentication:    "message/x.rsocket.authentication.v0",
		MessageZipkin:            "message/x.rsocket.tracing-zipkin.v0",
		MessageRouting:           "message/x.rsocket.routing.v0",
		MessageCompositeMetadata: "message/x.rsocket.composite-metadata.v0",
	}
	_mimeTypesReverse = make(map[string]MIME, len(_mimeTypes))
	for k, v := range _mimeTypes {
		_mimeTypesReverse[v] = k
	}
}

func (p MIME) String() string {
	return _mimeTypes[p]
}

// ParseMIME parse a string to MIME.
func ParseMIME(str string) (mime MIME, ok bool) {
	mime, ok = _mimeTypesReverse[str]
	if !ok {
		mime = -1
	}
	return
}
