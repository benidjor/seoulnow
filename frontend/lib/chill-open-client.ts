export type CongestGrade =
  | "여유"
  | "보통"
  | "약간 붐빔"
  | "붐빔"
  | "알수없음";

export interface ChillOpenPlace {
  biz_reg_no: string;
  name: string;
  category?: string | null;
  district: string;
  gu_code?: string | null;
  latitude: number;
  longitude: number;
  open_hour: number | null;
  close_hour: number | null;
  avg_congest_score: number;
}

const COLOR_BY_GRADE: Record<CongestGrade, string> = {
  여유: "#28a745",
  보통: "#ffc107",
  "약간 붐빔": "#fd7e14",
  붐빔: "#dc3545",
  알수없음: "#9ca3af",
};

export function mapCongestScoreToGrade(
  score: number | null | undefined,
): CongestGrade {
  if (score === null || score === undefined || Number.isNaN(score)) {
    return "알수없음";
  }
  if (score < 1.5) return "여유";
  if (score < 2.5) return "보통";
  if (score < 3.5) return "약간 붐빔";
  return "붐빔";
}

export function congestGradeToColor(grade: CongestGrade): string {
  return COLOR_BY_GRADE[grade];
}

interface OpenHours {
  openHour: number | null;
  closeHour: number | null;
}

export interface OpenFilterOptions {
  closingBufferMin?: number;
}

export function isOpenAtHour(
  hours: OpenHours,
  currentHour: number,
  options: OpenFilterOptions = {},
): boolean {
  const { openHour, closeHour } = hours;
  if (openHour === null || closeHour === null) return false;

  const buffer = (options.closingBufferMin ?? 0) / 60;
  const effectiveClose = closeHour - buffer;

  return currentHour >= openHour && currentHour < effectiveClose;
}

export function filterChillOpenPlaces(
  places: ChillOpenPlace[],
  currentHour: number,
  options: OpenFilterOptions = {},
): ChillOpenPlace[] {
  return places.filter((p) =>
    isOpenAtHour({ openHour: p.open_hour, closeHour: p.close_hour }, currentHour, options),
  );
}
