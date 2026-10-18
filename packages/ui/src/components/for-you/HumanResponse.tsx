import { useRef, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { missionControlAuthHeaders } from "../mission-control/missionControlAuth.js";

export interface HumanQuestion {
  id: string;
  question: string;
  options: { id: string; label: string; recommended?: boolean; consequences?: string }[];
}
export type HumanAnswers = Record<string, string | { kind: "typed-reply"; text: string; unansweredCount: number }>;
type Response = { kind: "choice"; questionId: string; optionId: string } | { kind: "custom" | "clarification"; text: string };

/** Explicit operator answers on the native human decision, using the existing local admin bearer. */
export function HumanResponse({ qitemId, destinationSession, questions, answers, hostId, requestRevision, actorSession, forwardingStatus }: {
  forwardingStatus?: "pending" | "awaiting-origin" | "origin-applied" | "conflict";
  qitemId: string; destinationSession: string; actorSession?: string; requestRevision: string; questions: HumanQuestion[]; answers: HumanAnswers; hostId?: string;
}) {
  const client = useQueryClient();
  const [enteredActor, setActor] = useState("");
  const actor = actorSession ?? enteredActor;
  const [mode, setMode] = useState<"custom" | "clarification" | null>(null);
  const [text, setText] = useState("");
  const [receipt, setReceipt] = useState("");
  const [closed, setClosed] = useState(false);
  // A lost HTTP response may already have committed. Retry the same content with its same id.
  const retryIds = useRef(new Map<string, string>());
  const mutation = useMutation({
    mutationFn: async (response: Response) => {
      const key = JSON.stringify([actor.trim(), requestRevision, response]);
      let replyId = retryIds.current.get(key);
      if (!replyId) { replyId = crypto.randomUUID(); retryIds.current.set(key, replyId); }
      const res = await fetch("/api/mission-control/human-response", {
        method: "POST", headers: { "Content-Type": "application/json", ...missionControlAuthHeaders() },
        body: JSON.stringify({ qitemId, replyId, actorSession: actor.trim(), requestRevision, hostId: hostId ?? "local", response }),
      });
      const result = await res.json();
      if (!res.ok) throw new Error(result.message ?? `HTTP ${res.status}`);
      return result as { replyQitemId: string; complete: boolean; forwardingStatus?: "pending" | "origin-applied" };
    },
    onSuccess: (result, response) => {
      setReceipt(result.forwardingStatus === "pending" ? "Your response was recorded for forwarding. Origin delivery is pending." : response.kind === "clarification"
        ? "Question sent to the manager. The decision remains pending."
        : result.complete ? "Your answer was recorded and sent to the manager." : "Your answer was recorded. Other questions remain unanswered.");
      setClosed(result.complete || result.forwardingStatus === "pending");
      setMode(null); setText("");
      client.invalidateQueries({ queryKey: ["queue", "item", qitemId] });
      client.invalidateQueries({ queryKey: ["mission-control", "view"] });
    },
  });
  const disabled = mutation.isPending || !actor.trim() || closed || Boolean(forwardingStatus);
  const buttonClass = "min-h-[44px] border border-outline px-3 py-2 text-sm disabled:opacity-50";
  return <section aria-label="Human decision response" className="space-y-3">
    {actorSession ? <p className="text-sm">Responding as {actorSession}.</p> : <label className="block text-sm">Operator identity
      <input aria-label="Operator identity" value={actor} onChange={e => setActor(e.target.value)} disabled={mutation.isPending || closed}
        placeholder={destinationSession} className="mt-1 block w-full border border-outline bg-background p-2" />
    </label>}
    {!actorSession || actorSession !== destinationSession ? <p className="text-sm">This request is addressed to {destinationSession}.</p> : null}
    {questions.map(q => {
      const answer = Object.prototype.hasOwnProperty.call(answers, q.id) ? answers[q.id] : undefined;
      return <fieldset key={q.id} className="space-y-2">
        <legend>{q.question}</legend>
        {answer ? <p className="text-sm">Recorded answer: {typeof answer === "string" ? q.options.find(o => o.id === answer)?.label ?? answer : answer.text}</p> : null}
        {q.options.map(o => <div key={o.id}>
          <button type="button" disabled={disabled} className={buttonClass}
            onClick={() => mutation.mutate({ kind: "choice", questionId: q.id, optionId: o.id })}>
            {o.label}{o.recommended ? " (Recommended)" : ""}
          </button>
          {o.consequences ? <p className="mt-1 text-sm">{o.consequences}</p> : null}
        </div>)}
      </fieldset>;
    })}
    <div className="flex flex-wrap gap-2">
      <button type="button" className={buttonClass} disabled={disabled} onClick={() => { setMode("clarification"); mutation.reset(); }}>Ask a question</button>
      <button type="button" className={buttonClass} disabled={disabled} onClick={() => { setMode("custom"); mutation.reset(); }}>Write a custom response</button>
    </div>
    {mode ? <div className="space-y-2">
      <label className="block">{mode === "clarification" ? "Your clarification question" : "Your custom response"}
        <textarea aria-label={mode === "clarification" ? "Your clarification question" : "Your custom response"}
          value={text} maxLength={10000} onChange={e => setText(e.target.value)} disabled={mutation.isPending}
          className="block w-full border border-outline bg-background p-2" />
      </label>
      <p className="text-sm">{mode === "clarification" ? "Asking a question keeps this decision pending." : "A custom response answers the first unanswered question. Any remaining questions stay pending."}</p>
      <button type="button" className={buttonClass} disabled={disabled || !text.trim()} onClick={() => mutation.mutate({ kind: mode, text: text.trim() })}>
        {mode === "clarification" ? "Send question" : "Send custom response"}
      </button>
    </div> : null}
    {forwardingStatus ? <p role="status">{forwardingStatus === "pending" ? "Your response was recorded for forwarding. Origin delivery is pending." : forwardingStatus === "conflict" ? "The origin question changed. This response awaits reconciliation." : "The origin recorded your response. Awaiting the next verified question."}</p> : null}
    {mutation.error ? <p role="alert">{mutation.error.message}</p> : null}
    {receipt ? <p role="status">{receipt}</p> : null}
  </section>;
}
