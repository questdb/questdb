import { danger, fail } from "danger";
import { validate } from "./validate";

validate({ title: danger.github.pr.title, onError: fail });
