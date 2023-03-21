
//const consume = require("./consumer");
const consume = require("./consumerAvro");
//const consume = require("./consumerAvroFromFile");

//import {consume} from "./consumerAvro";

consume().catch((err) => {
    console.error("error in consumer: ", err);
})