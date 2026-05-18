import { z } from "zod";

export const autoOpsWebhookSchema = z.object({
  source: z.string().optional(),
  resourceId: z.string().min(1),
  resourceName: z.string().min(1),
  title: z.string().min(1),
  description: z.string().optional(),
  severity: z.string().optional(),
  status: z.string().min(1),
  message: z.string().optional(),
  startTime: z.string().optional(),
  endTime: z.string().nullable().optional(),
  endpointType: z.string().optional(),
  affectedNodes: z.union([z.array(z.string()), z.string()]).optional(),
  affectedIndices: z.union([z.array(z.string()), z.string()]).optional(),
  eventLink: z.string().optional(),
});

export type AutoOpsWebhookInput = z.infer<typeof autoOpsWebhookSchema>;
