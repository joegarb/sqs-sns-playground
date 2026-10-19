import { CloudFormationClient, DescribeStacksCommand } from "@aws-sdk/client-cloudformation";

import { STACK_NAME } from "../topology.ts";

export async function stackOutput(key: string): Promise<string> {
  const cfn = new CloudFormationClient();
  try {
    const { Stacks } = await cfn.send(new DescribeStacksCommand({ StackName: STACK_NAME }));
    const value = Stacks?.[0]?.Outputs?.find((output) => output.OutputKey === key)?.OutputValue;
    if (!value) throw new Error(`Stack ${STACK_NAME} has no output ${key}. Run \`npm run deploy\` first.`);
    return value;
  } finally {
    cfn.destroy();
  }
}
