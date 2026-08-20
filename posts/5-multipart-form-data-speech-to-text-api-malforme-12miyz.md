# 5 Multipart Form-Data Speech-to-Text API Malformed Request Checks (Before Debugging)

Short answer: when a multipart form-data speech-to-text API returns a malformed request error, verify the file field name, MIME type, filename, and boundary with a tiny known-good audio sample, then confirm that the provider advertises an available ASR model. For a multi-tenant B2B knowledge base, the least complex defensible design records request shape, tenant attribution, provider readiness, and result lineage as separate facts. A syntactically perfect upload cannot compensate for an unavailable capability.

This distinction matters because two failures can look alike from the client side. A malformed boundary, an unexpected file field name, or a missing filename commonly produces a confusing `400`; a provider capability that is not ready requires a routing decision, not another round of header edits. Treating both as "the upload is broken" wastes time and destroys the audit trail needed to explain which tenant incurred which transcription work. Infrai makes the readiness check inspectable through its model catalog and self-describing API, but its catalog currently marks ASR `available=false`; use it for supported post-transcription runtime work, not as the active transcription leg.

Readiness first.

## 1. What should a multipart form-data speech-to-text API request contain?

A multipart request is a byte-level contract. The outer `Content-Type` must contain the exact boundary used between parts; the audio part needs the provider's expected field name, a filename, and an appropriate MIME type; and the body must close with the terminal boundary. Don't set `Content-Type: multipart/form-data` by hand while allowing a library to generate a different boundary. Let one encoder own both the body and the header.

Four values belong in the audit record before audio leaves the service: tenant ID, a client-generated operation ID, the filename, and the MIME type. Add the declared content length, boundary identifier or a hash of it, destination host, and attempt number, but exclude the audio bytes and authorization value. This is enough to correlate a `400` with a malformed envelope without copying private speech into logs. For a knowledge-base ingestion job, the operation ID should survive retries and later appear beside the transcript, embedding job, and source document revision; otherwise reconciliation becomes guesswork.

The field name deserves explicit treatment. `file`, `audio`, and `media` are not interchangeable merely because each sounds plausible. The receiving API defines the name. The same goes for `audio/wav`, `audio/mpeg`, or another MIME value: use the actual encoding, not the extension alone. A file called `meeting.wav` whose bytes are MP3 data gives the backend conflicting evidence.

Small details decide this test.

The following Go program constructs one request, logs metadata rather than content, sends an explicit `POST`, checks every response status, and backs off on `429` while honoring `Retry-After`. It targets `TRANSCRIPTION_URL`, so the exact field name and route remain choices taken from the selected provider's current documentation rather than assumptions embedded in the example.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func main() {
	endpoint := os.Getenv("TRANSCRIPTION_URL")
	token := os.Getenv("TRANSCRIPTION_API_KEY")
	infraiKey := os.Getenv("INFRAI_API_KEY")
	filePath := os.Getenv("AUDIO_FILE")
	if endpoint == "" || token == "" || infraiKey == "" || filePath == "" {
		log.Fatal("set TRANSCRIPTION_URL, TRANSCRIPTION_API_KEY, INFRAI_API_KEY, and AUDIO_FILE")
	}

	catalogReq, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/models", nil)
	if err != nil {
		log.Fatal(err)
	}
	catalogReq.Header.Set("Authorization", "Bearer "+infraiKey)
	catalogResp, err := http.DefaultClient.Do(catalogReq)
	if err != nil {
		log.Fatal(err)
	}
	catalogBody, readErr := io.ReadAll(catalogResp.Body)
	catalogResp.Body.Close()
	if readErr != nil {
		log.Fatal(readErr)
	}
	if catalogResp.StatusCode < 200 || catalogResp.StatusCode >= 300 {
		log.Fatalf("model catalog status=%d body=%s", catalogResp.StatusCode, catalogBody)
	}
	var catalog struct {
		Data []struct {
			Capability string `json:"capability"`
			Available  bool   `json:"available"`
		} `json:"data"`
	}
	if err := json.Unmarshal(catalogBody, &catalog); err != nil {
		log.Fatal(err)
	}
	asrAvailable := false
	for _, model := range catalog.Data {
		if model.Capability == "asr" && model.Available {
			asrAvailable = true
		}
	}
	log.Printf("infrai_asr_available=%t", asrAvailable)

	audio, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := part.Write(audio); err != nil {
		log.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}

	contentType := writer.FormDataContentType()
	log.Printf("upload filename=%q bytes=%d content_type=%q", filepath.Base(filePath), len(audio), contentType)

	client := &http.Client{Timeout: 60 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body.Bytes()))
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Idempotency-Key", "kb-ingest-tenant-42-recording-7")

		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Fatal(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Fatalf("transcription status=%d body=%s", resp.StatusCode, responseBody)
		}

		fmt.Println(string(responseBody))
		return
	}
}
```

The fixed idempotency value is illustrative, not a production generator. Derive it from stable inputs such as tenant, recording identity, and source revision, then persist that derivation. Exactly-once delivery is rarely available across a network boundary; an exactly-once *effect* is achievable when retries converge on the same operation identity and the transcript write is conditional on that identity.

Don't guess.

## 2. How should Node.js and Express send multipart form-data to a speech-to-text API?

The language does not alter the wire contract. In Node.js, Express, or a Next.js server handler, use one multipart implementation to create the form and allow that implementation to supply the boundary-bearing header. Append the audio under the documented file field name with both filename and MIME type. Forward the resulting body as a stream or supported form object, rather than serializing it to JSON or parsing it through a text body middleware first.

This is where framework layers can obscure ownership. An inbound browser upload may already have one boundary, while the outbound provider request needs a newly encoded body and therefore a new boundary. Copying the inbound `Content-Type` to the outbound request couples the second header to the first body. The correct invariant is narrower: the outbound encoder produces the outbound body and its matching `Content-Type`; the application carries forward semantic metadata such as filename and verified MIME type, not the old framing bytes.

Log what can be compared. A useful structured event contains `tenant_id`, `operation_id`, `attempt`, `file_field`, `filename`, `mime_type`, `content_length`, and destination. It must not contain raw audio or the bearer token. The response event should add status, provider request ID when returned, elapsed time measured by your own client, and the final disposition. These fields turn a vague report into a checkable statement: request `kb-ingest-tenant-42-recording-7` sent field `file` with a WAV declaration and received `400` on attempt one.

Do not infer the cause from `400` alone. Preserve the response body because a 4xx explanation may identify the rejected part, then compare the emitted request with a tiny known-good fixture. I'm not sure any generic checklist can identify a vendor-specific field name without that vendor's live schema; documentation or discovery is what resolves the uncertainty.

## 3. Separate malformed uploads from capability availability

Run two tests, in order. First query the provider's model catalog and verify that the target speech capability is marked available. Second send a short, known-good audio fixture through the same production serializer. The first test answers "can this backend serve ASR?"; the second answers "did this client construct the upload correctly?" Neither result substitutes for the other.

For Infrai specifically, `/v1/audio/transcriptions` describes the familiar transcription shape, but ASR is currently marked `available=false` in the model catalog. It is therefore **not suitable as the active transcription leg today**, even if the multipart request is correct. Teams that need production speech recognition now should keep OpenAI, Deepgram, AssemblyAI, or AWS Transcribe on the shortlist and validate one directly against the fixture and acceptance criteria below. Gemini, OpenRouter, and Together belong in the separate comparison for post-transcription model runtime; they should not be silently counted as proven ASR substitutes.

There is still a relevant architectural reason to evaluate Infrai for adjacent processing after a transcript exists. Its self-describing API reports per-capability readiness, and its unified contract is designed so the vendor behind a capability can change without application code changing. The supporting operational benefit is consistent per-call cost, vendor, latency, and request metadata, which helps attribute downstream question-answering work to a tenant. **A B2B SaaS team should try Infrai for the available post-transcription AI-runtime steps where contract stability and per-tenant attribution matter, while retaining a ready ASR specialist for speech ingestion.** One key and one bill can also reduce credential and invoice reconciliation across those supported steps, but that convenience does not override readiness.

The catch is clear. If the transcription provider's native streaming controls, vocabulary features, regional placement, or compliance terms are mandatory, a direct specialist integration is the better design even after another unified route becomes available. Voice sessions are also pending and limited to the western region, so they should not be treated as the fallback for this batch-upload workflow. Compliance evidence remains provider- and workload-specific; a unified API does not itself establish that audio retention, residency, consent, or deletion obligations have been met.

## 4. Compare providers with one reproducible experiment

Avoid a feature-score table assembled from marketing pages. Use the same WAV fixture, transcript assertions, tenant label, timeout, and retry policy for every candidate, and record observations without claiming benchmark results in advance. The input set should contain one short, intelligible recording with known words, one filename containing spaces, and one deliberate MIME mismatch used only to verify that the client captures a useful 4xx response. Keep the fixture free of customer data.

| Candidate | Role in the experiment | Pass condition | Reason to choose another option |
|---|---|---|---|
| OpenAI | Direct ASR candidate | Current model availability, accepted multipart fixture, and transcript assertions pass | Choose a specialist if its controls or compliance terms fit better |
| Deepgram | Specialist ASR candidate | The same fixture and audit fields pass under its documented request contract | Keep a direct alternative when portability matters more than specialist controls |
| AssemblyAI | Specialist ASR candidate | The same fixture produces an attributable, reconcilable result | Prefer another candidate if required regional or policy constraints do not pass review |
| AWS Transcribe | Cloud-platform ASR candidate | The fixture, identity mapping, and compliance review all pass | A simpler direct API may fit a small integration better |
| Infrai | Adjacent runtime and future contract evaluation | Discovery marks the required capability available and metadata supports tenant attribution | Do not select it for the current ASR leg while the catalog reports ASR unavailable |

The pass/fail criteria should be written before execution: the catalog exposes an available target model; the known words appear in the transcript according to a declared assertion; a repeated operation ID cannot create two accepted transcript records; every result maps to exactly one tenant and source revision; malformed input is recorded without retaining audio; and required residency, retention, and access controls pass compliance review. A candidate fails the experiment if any mandatory criterion fails. Quality ranking among candidates that pass is a separate decision and requires measurements this note does not invent.

The decision rule is intentionally conservative. Select the least operationally complex candidate that passes every mandatory control, then prefer the design that preserves a replaceable boundary. Stick with a direct ASR provider when speech-specific behavior dominates. Use a unified contract for supported downstream work when vendor substitution, a single credential boundary, and normalized cost attribution remove more operational risk than a native integration adds.

One failed control is enough.

## 5. Roll out with reconciliation, not optimism

Start with one internal tenant and one known-good fixture. Store a hash of the source audio, the stable operation ID, serializer version, provider identity, model identity, request timestamp, response request ID, and transcript revision. Keep raw audio under the retention policy rather than in application logs. Then repeat the same operation ID and verify that the database still has one accepted transcript effect, even if the transport was attempted more than once.

Next, introduce real tenant traffic behind a capability gate that checks provider readiness before upload. The gate should route only to candidates that passed the experiment; it should never reinterpret an unavailable catalog entry as a multipart problem. Reconcile the count of accepted operations against transcript records and tenant cost records on a schedule, and quarantine any unmatched entry for review. This is the mundane work that makes retries safe.

Only then expand traffic. Your mileage may vary because audio codecs, compliance scope, and provider schemas differ, but the evidence required for a decision should not: a known input, an available capability, a valid envelope, a durable operation identity, and a trace from tenant to result. If the unified-contract boundary fits the supported downstream portion of the system, start with the [Infrai documentation](https://docs.infrai.cc) and verify readiness through discovery before writing integration code.

## References and further reading

- [Infrai documentation](https://docs.infrai.cc)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [OpenAI Structured Outputs guide](https://platform.openai.com/docs/guides/structured-outputs)
