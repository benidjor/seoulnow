/**
 * Edge API fetch wrapper — degraded 상태를 명시적으로 노출.
 *
 * Edge API 는 upstream 미연결 시 503/502/504 + JSON body + `x-degraded-reason`
 * 헤더를 반환한다. `res.json()` 이 성공해도 `res.ok` 가 false 이면 degraded 로
 * 판정해서 frontend 가 "로딩 중" 과 "데이터 source 미연결" 을 구분하게 한다.
 */
export interface ApiResult<T> {
  data: T;
  degraded: boolean;
  reason: string | null;
}

export async function fetchApiJson<T>(url: string, fallback: T): Promise<ApiResult<T>> {
  try {
    const res = await fetch(url);
    const reason = res.headers.get("x-degraded-reason");
    const data = (await res.json().catch(() => fallback)) as T;
    return { data, degraded: !res.ok, reason };
  } catch (e) {
    return {
      data: fallback,
      degraded: true,
      reason: e instanceof Error ? e.message : "fetch_failed",
    };
  }
}
