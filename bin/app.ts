import { App } from "aws-cdk-lib";

import { PlaygroundStack } from "../lib/playground-stack.ts";
import { STACK_NAME } from "../src/topology.ts";

const app = new App();
new PlaygroundStack(app, STACK_NAME);
