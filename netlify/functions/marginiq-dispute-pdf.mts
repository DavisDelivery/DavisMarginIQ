import type { Context, Config } from "@netlify/functions";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";

// Davis MarginIQ — Dispute Package PDF Generator
// POST body: {
//   items: [{ pro, customer, billed, paid, variance, pu_date, category, code, city, zip }],
//   customer: string,
//   ap_contact: { billing_email, ap_contact_name, ap_contact_phone }
// }
// Returns PDF bytes as base64 for client to download.

export default async (req: Request, context: Context) => {
  if (req.method !== "POST") {
    return json({ error: "POST required" }, 405);
  }

  try {
    const body = await req.json();
    const items = body.items || [];
    const customer = body.customer || "Unknown Customer";
    const apContact = body.ap_contact || {};

    if (items.length === 0) {
      return json({ error: "No items provided" }, 400);
    }

    const pdf = await PDFDocument.create();
    const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
    const font = await pdf.embedFont(StandardFonts.Helvetica);

    const BRAND = rgb(0.118, 0.357, 0.573);  // #1e5b92
    const RED = rgb(0.937, 0.267, 0.267);
    const DARK = rgb(0.06, 0.09, 0.16);
    const MUTED = rgb(0.39, 0.45, 0.55);

    let page = pdf.addPage([612, 792]); // letter size
    const { width, height } = page.getSize();
    let y = height - 50;
    const margin = 50;

    // Header — blue band with logo on left, claim type on right
    // Logo is the Davis Delivery Service wordmark + truck, embedded as JPEG.
    page.drawRectangle({ x: 0, y: height - 90, width, height: 90, color: BRAND });
    const LOGO_B64 = "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAUDBAQEAwUEBAQFBQUGBwwIBwcHBw8LCwkMEQ8SEhEPERETFhwXExQaFRERGCEYGh0dHx8fExciJCIeJBweHx7/2wBDAQUFBQcGBw4ICA4eFBEUHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh7/wAARCACMARgDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD7DApcUL0paACkwPSlooAKTApaKAExS0UUAFJS0UAFJilooASloooAKTFLRQAUUUUAJiloooAKKKKACkxS0UAJiloooATAoxS0UAFGKKKACiiigApMClooAb0opWooEC9KWkWloGFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFACGig0UAC0tItLQAUUUUAFFJRmgBaK5TWviP4E0a9kstT8W6Pa3MTbJImuQWRvQgZwfaq0PxW+HEjbR400VT/00uNn/oWK1VGo1flf3GTrU07cy+87SisfSvFHhrVWVdL8Q6RfM3QW97HIT+AOa2O9ZuLW5aknsFFFFIoralf2emWUl7qF3BaWsQBkmmcIi5OBknpyQKxR488FHp4s0P8A8DU/xrG+P4z8Idf/AOucX/o5K+SLKzuL69hsrKB57meQRxRIMs7HoB717WW5XTxdJ1JStZ/oeNmOZzwtVQjG90faUfjjwa7BU8V6IzHt9tj/AMa2rK7tb2ET2dzBcRHo8UgdfzHFfGk/w68eQxmSTwhq21eSVg3foCTWVoura34a1Mz6XeXel3kTYcJlDn0ZTwfoRXU8ipVF+5q3fy/Q5FndWD/e0rL+u59z0V518FPiOnjjTJbW+jjg1mzUGdE4WZDwJFHbngjscdiK9ErwK1GdCbhNWaPfo1oVoKcHdMz9a1vR9FSOTV9UstPSVisbXMwjDkDJAz1qrp3i7wvqN7HZaf4i0q7uZSRHDDdKztgZOADzwDXzd+0j4k/tvx++nQybrTSE+zrg8GU8yH88L/wGvPtD1K50bWbLV7M4uLOdZ4/cqc4/Hp+Ne7h8i9rQVSUrSavb8jwq+eOnXcFG8U9/zPuXUL200+ylvb65itraJd0ksrBUQepJ6CsQeOvBZIA8WaJk8AfbU/xq5DJpvivwmsgxLp+q2fI/6ZyLyPqM/mK+KvEWk3Gi65f6NeDE1nO8D/7WDgH8Rg/jXHluX08W5RnJpo7MxzCeFUZQSaZ91jkUZrjPgp4i/wCEl+HWm3ckm+6t0+yXOevmR4GT9V2n8a3vGGtQeHfDGo63cYKWVu0oB/iYD5V/FiB+NedOjOFV0ut7HoQrRlSVW+lrle88Z+ErO6ltLvxNo8E8LlJYpLtFZGHUEZ4NWofEGhzaPJrMWsWL6bHnfdrOpiXBwct04JAr4gc3epaiztunvLubJ7l5Hb+rGvqPx3oMXhn9njUNChwfsmnKkjD+KQupdvxYmvWxmV08O6ceZtydv8zysJmdTEKpLlSUVc7G28aeEbq5jtrbxPo800rhI40u0LOxOAAM8kmt7vXxJ8PePH3h7j/mKW//AKMWvqH44+Lrjwf4HmvLBguoXUotbVyM+WzAkvj2UEj3xWeNyv2FaFKm7uXc0wWZ+2ozq1FZRN7xJ4v8MeHGCa3rljYyEZEUkn7wj12DLfpWPZ/FT4e3cwii8VWKsTgeaHjH5soFfJmjaXrPijX1srCKbUNTu2LEu+Wc9WZmY9O5JNdRrPwk+IOlWjXMuhG5jUZb7JMszD/gIOT+ANdzybCU7Rq1bSfojhWcYqpeVKleK9WfXVpdW13bpcWs8U8LjKSROHVvoRwamr5h/Zh0/wAQ3HjCe4s726s9Ish/p8Q+5M5yFiKngN3J6gD3r6eFeNjsKsLVdNSuezgcU8VSVRxsFFFFcZ2CGihqKQAvSlpF6UtMApCcUtUdU0y11NBFfB5rf+KAuRHJ/vgfeHsePahCd+hzGr+OJLmaTTvBOkS+Jb9SUeaN/Lsbc/8ATS4Pykj+6m5vpXlvxK0PxjB4r8Eah4q8Vm/lvtfhj/s6yQw2Vuq4fCjOXPy/ebtX0FbwxQQpBBGkUUa7URFCqo9ABwK8x+OSbvEHw/P9zXJH/wC+baRv6V24WolUSiu/rt/WxxYqm3Tbk+3pv/W58jaV4j1mx1ie506cyPczPJLbyRCeKfcxJDxMCHBz6Z9xX1n8E9Q/tLw3b3I0yWwtCfJutJv42As5MfftmlGWgb+4SdvY8EH5W+HiS6lqKaa1/eW0UghGbaYxsN9xDGTkdTtkbr3xXvup/CTwXo3xc8M6TeW17qel6xZXkZS9vZJCbmIK6tuBB5Xdx0r2MxVOT5Ho99PJfI8rAe0XvrVf5/eeyX/hDwdqoIvfDWh3ZP8AE1nEzfgQM1hTxXHw6niuYbi5uPCEsix3ENw7SPpLMcLIjtljBkgMpJ2ZDA7cgVz8EfhmcbPDLwnsYr24Q/o9RX3wV8KS2E9laX3iawinjaJli1qdkKsMEFXYgjnoa8WMqWzk2vNf8E9eUam6ir+v/APSgaWuX+Fd7LfeANHNzn7VbQfYrnJyfOgYwvn/AIEhP411Fc0o8smux1QlzRT7nB/tAOkfwg193YKoiiyT0H75K+ZPhg6H4l+GcMpzqkGOevzivpL9pU/8WQ8S/wDXGL/0dHXyJ8HT/wAXc8J/9he3/wDQxX0eUz5cHUXr+R89mlPmxdN+n5n38o4rxP8Aav8ADlq3g4eL7a3jW/sJo453AwZYXO3DHuVYqQfTIr20HivFP2vfE9lpvw5Hh3zkN/q08ZWHPzLDGwdnI7DIVR6k+xrxsvlOOJhyb3/Dqetj4wlh5qe1vx6Hi3wE8Tix+LGg7d8Zu7j7HIvUMsgK4/Pafwr638feII/DHg7UtckwWtoCYlP8cp4RfxYiviv9n/TJtX+MnhqGJSRBeC7kP91IgXJP4gD8a9x/af8A+En8X6xpXgLwlpd5ftDi9v2hGI0Y5WJXc4VcDc3J7rXr5lCNbGQUtra+lzy8vlKjhJuO99PU8TsLW+1/XYLKJmmvdQuQgY8lpHblj+JJrsfjn4Pg8HeLIYLJdunXVrG8BPTcoCSD65G7/gdWfBuiaD8HfEkHiHx/4rtbzV7aJ/s+h6bm5lSRht3O2QFIBOM4HOc8VuD4g+Pfi1qIXwb4Y0rRdNsWYnW9UjWc2gIG4h2GxWwBwoY8dR1rtqZhP20Z01+7S1votf66HDTy+HsnCb99vTqzsv2c/EL2Xw9ntfERbTbKwn/0W7vR5MUkb5O1WbAYht3A/vCuX/aa0fw3bpH46k1C9jk1FFt4bWCz4uJVU4dnYjYNoHUEnHGa57RfHfg/Q/idosMt5ceNLs3awX/iTVpC6w7jtH2SMnbGoYqS/cZxxzXuPx58Kt4u+F+radFH5l7bp9rsx382LJwP94bl/GvKlVlQxiq/Cpfrv/merGlGthHSfvcv9f8AAPG/2R/Gca+KL7wtPujXUIftEAJyPNjHzAfVCf8Aviux/as8Q/Z9F07wzC+JLyT7VcAH/lmhwoP1c5/4DXyv4P1258OeKtL1+zz5tjcpOB03KD8yn6qSPxrqPjJ46l8WfEXU9Xsbhv7P3LBZhh/yxQYBwemTub8a9N4Tmx6rtaWv81oecsS1gXRjve3yPQv2cfDg1z4iRX00e610lPtTk9DJ0jH55b/gNe8fHIf8Wm8RD/p1H/oa1gfsveHptH+GFtqV6uL3WW+2PlcEREYiX/vn5v8AgVbH7Qcrw/BnxPLGQGWzBGR/00SvKxeK9vj4tbJpL7z0sJhfYYGSe7Tf4HzB8Pjjx/4e/wCwpb/+jFr6G/ad0O71XwBHe2cbytpl2LiVFGT5RUqzY9sg/TNfKvw3vri4+JnhhZZSV/ti14HA/wBatffN/cW9paz3V3LHDbwo0kskhwqIASxPsBmuzN8S6WKpVIrY5MqwyqYapTk9z4j8GeJNR8KeILfW9KaL7RECu2VdyOjDBUj0PtXuXh79oXS5ikev6FdWbd5rSQTJ9dpww/WtfxB8HfAni+2TWdAupNM+1r5sVxprq9vKDzuCEFcf7pFeQ/E74NeLfB+iXeuWWoWGs6daIZJysTQzxoOrbCSGA74P4VtPE5fj2lVVpbdv+B95lDDY/Ap+zd479z6b8H694a8Q2kt/4cvLS4SSTfceSoVw5AGZFwGDYAGT6Vu18CfC7xpq3hjx9pWqwXLiI3CQ3US8LLC7BWUjvwcj0IBr77HGR6HFeHmOD+q1LRd09j2sBi3iKd5KzQUUUVwHeIelFB6UUAC9KWkXpS0AFJS1Vv72OzQM8NzKT0WGFpCfyoE3Ys15l8b1zrPgo/3dRu2/KxnP9K0tW+JNlZs6R6Rdyupx++vrK2H/AJEnB/SuA8T+MG8Xa/okcy+HdLjspLpkB8S2txPPJLaywxxrHGTyXde5rsw9GalzPbX8jjxFaEo8q30/M+YPDV1LaRXk8ErxTJYho5EOGVlliYEH1BGa19b8f+M9bfT31XxJqF2+nymW1cyBJInIwSHUBs4461NafDn4hxRMg8Fa6N8Xltm1Ix09fpTrfQ9N8LFLnx1JcaddSuyWunjT1upcKcNLJG0iKEz8oBJLENgcZr6idSjfmdm/vZ85CFX4VdL7je8L/E9bYquuR6ze4PLyatdP/wCgSoR/499K9l8CeL/AniO1v53vfEOhRafCktzef8JFc/Zow7bVUs7hgxPRSvbqa818I2eneMNRvvC138PtH1aeC1W6ttT8OSpp88kDY2yqjkJJww+U4IOQRkVh2+jz+H9B+JXh24ju43gtLG4UXUHky7Vu0wWXJAOJOxIPYkV59SlSq3Sunp16Nr+tTtp1KlKzdmtenZf1se76b4l8K+HY5h4U+KuiXCzTvcS2etXKyrJI5y7CZdroSeed4z2r0LwL4rh8TWtxmBLe5tinmrFcLcQyI67klilXh0YA4OAQVIIBFfn6+c96+l/2d/EN9oyaPaHRZrjSb7TLSK5v42AWzlNzcpEGB6hiwHHI69Kwx2XqnT5k7v5G+Dx7nPlasj0n9pfj4H+Jf+uMX/o6OviPR9SvdG1i01bTZfJvLOZZ4JCoba6nIODwfxr9EfEmiaX4j0W50XWrRbuwuQBNCzMoYBgw5Ug9QD1rif8AhRvwq/6FC3/G5n/+LrHAZhSw9Jwmm7s3x2CqV6inBpWR8x3Hx2+Ks8LRnxOYwwxuisoVb8Dsrh559d8U69mWTUNa1a7YDktNNIewxyf6CvtaP4I/CtGBXwdaHH96aZh+Reut8OeGPDnhyIxaDoen6YrDDG2t1Qt9SOT+Jrf+1cPTV6VOz+S/Iw/s2vUdqs9Pmzxr4PeEdI+C/hO78ZePL2Cz1W8jEXl53tAnUQoB9+RiAWxxwB0BNeafFT9oDxL4mafT/Dfm+H9IYkFo3xdTj1dx9wey/ixr6c8Y/Dvwb4vv4r3xLow1KeFPLiMlzKFjXuFVWAGe5Aye9Y9t8E/hZb3EVxF4PtN8Th13zSsuQcjKlyCPY8VzUsZQ53VrJyk/SyOirha3KqdJpR/E8K+BXwLufFSQ+JfGCz2ujyESQWuSs16Ou5j1WM+v3m7YHJ+orvw5o83hSfwwljBb6VNavaG3hjCokbKQcAcd8/WtZdoAAxjtilzXJicXUxE+aT228jqw+Fp0I8q+Z+cHiTSLrQdf1DQ78EXNjcPbyH1KnGR9Rgj619y/AjxYfGPwx0nVZX3XsKfZLz/rtHhST/vDa3/Aqk8TfCn4feJNan1rXPDUF3f3G3zZjLKhfaAoyFYDoAK1fBfg3wz4Mtrm28NaYunQ3MgkmRZXcMwGAfmY4OOOK68bj6eJpRVnzL+mcuDwVTD1G7rlf9I+Lfjz4V/4RD4o6tpsMe2znk+2WeBgeVLlsD/dbcv4VjfDbw1N4w8daT4djVtt5cATMP4IV+aRvwUH8cV9x+M/h94O8ZXdvdeJdCh1Ge3jMcTtI6lVJyR8rDIz61B4P+GvgXwpqp1bw5oEFleGJofOWWRztJGQNzEDoPeuqOcJUOWz5rHPLKm63NdctzrbaGK3t47eCNY4YkCRoowFUDAA+gFcF+0X/wAkT8U/9eQ/9GJXoGaoeINH03xBo11o2r2ou7G6TZPCWIDrkHGVIPUDoa8SlPkqRk+jPYqR54OK6o+BPhdx8TvC/H/MYtf/AEatfT37XDeNJPBcdn4f0+WfRpHLatLb5eUKDlVKDny+7EZ6AHA69bpnwa+Gem6na6lY+FYIbu1lWaGQXExKOpBU4L44I713+ffmvSxWYQqVoVYx+HuefhsDKnSlTlLfsfn54D+I3i/wUzf8I7rMkFs7Ze2kAlgY9zsbgH3GDXT+Nvjv468V+HJ9CuzptnaXSeXcm0tyryp3UlmOAe+MZr6k8VfCb4eeJp3udV8L2f2huWntt1vIT6kxkZ/GufsP2e/hZbyiY6Nd3S9Qs2oSsh/AEZrp/tHBzfPOn73ojn+oYqK5Iz0+Z8z/AAK8E6h428fWEUNu502xuI7jUJ8fJGisGCZ/vMRgD3J6CvvAc8+tUNB0bSdB06PTdG0620+zj+7DbxhFB9cDqfc81oV5uOxjxU+a1ktj0MHhVhoWvdsKKKK4jsENFB6UUAItOpq06gArw74++APEuteMbPxrpc1udP0bTHaWIzMJd8YlcMiAEMclSOeor3GqGr6RpurQeRqNpHcx4Iw+e/0IrWjVdKXMjKtSVWPKz4p8b+GvAVno3j660aa3ll07VtPh0pln35ikjzIBz84J35PONlcp8ObPVoNXg1620fU5rSIXESXdtZSSpDOYXVGygPKs6H1HWvsHUPgH8J70HPhOK2Y/xWtzLFj6ANj9KZYfBmx0awFj4X8ceNdAtVZmSC11FWiUk5J2uh6n3r1o5lBQcdXfv6WPLeXzc1LT5ep8Z22kXDRr9v1SHTJAOY79LmMj8fLI/Wvcf2Y/DvhfxprOv/2zaWmpw2Oh2OnwJKoYRh0bzWXPRt4b5uoJPrXp198MviUit/ZXxv1seiX2nwzA/UjH8q4W9+DvxvtNcv8AXtL+IGjXGoX9n9iuZhEbZpYewIEZUEdm6j1oqYuNeLjzJff/AJBDCOjJS5W/u/zPn/w1rg8P6j5tq18Lq0lkigvLTUXt5VTcRgEAjB5PTnNdtD8QS2rSanqM2uanPNZmxmXUJba7jlty2/y2V4hkBhuHcGrOlfAf4seHdZivYfCXh7WxGGURXVzFPbnIxlkZlJI7eldtY+BvigMfaPgl8LHH+0iIf/HZTXVPFUOtn8zljhq/S6+Rwb+NPCRGT4N03d/2CrQVY0DxhJq/jTQ9P07+0LeKfUrGNLKOeKK1xHKpQeRFGAQPmPXqSa0/GHwG+I3iTWRqNp4T8K+HVaJUe1stRIhLD+ILtO0kYyBwcZ65r0b4A/DDxV8OEu5r3wxouo6jdOubz+18GJFzhUUwnHUknPPHpUVMVh1TbSu+1y4YWu52k7L0Oy/ahbb8EtebeUw9tyGxx9oj71y3gweHdG+N+kaP8N9Wa70i60u4l1y0t79ru2gKgeTJkswRyxxgHp9a9knsoNY0prLXdKtJoZcebay7Z42wQRnIAPIz07Uui6Ho2iQtDo2kWGmxOcslpbJEGPqQoGa8aFdRpOD8/TW35HrzouVRT9PXT/M+adGs9Gvvix4ol1jTvDlyI/Fkii41HxJJZ3EKh1P7uAHEgB5Gep4r0zVNa0zR/wBpmabWNWs9Otm8Ioqvd3KxIW+1E4G4gZwD+Vd3c+CfB11fvqFz4U0Ka8eTzXnksImkZ853FiuSc85qp4+s/AsFlJr/AIx0jR7lIVEXn3ditxIQT8qKNpZiSThR6n3rSWJjOWqe1jOOHlBaNb3OX8TXCy/tE+BHgnDxSaLqLoUfKuCEIYY4Ix3rzjxkmr6P4u134Naetwtr4w1WC+0+ZSf9HtZSWvFB6gAxnAHYmvoi2sNHmlstTisLQywW+y0n8gB4omA+VDjKqRjgYpupR6FDqdnqOoJpsd8geG0uJ9iygFSzqjNz91SSB2BJ4FRTxCjbS9l+N7plzoOV9ba/hazR4Z+0HZaevxE8F6VJZaVcWEWk3SJbajqrWFvhSgXMqnIIAGB3qb4qW9lB+zz4ctNMsrI251izT7Hpupm5hZmmffElwTlskkZJ4J7Yr1i3tvBPj/TbfWH0vTNctgZIoJryxD4w2G2+YucEjOeh4Nakfh3w/HpcGlR6JpqWFvKJYbVbZBFHIG3BlTGAwJJyO9WsUkoK3wkvD3cn3PHPgBZ21/4h8fWC6ZLouir5VhN4cur9riSCXawkkOSdquDgEEg44PFM+D3h3VLj4kX2j67qzahpvw9c2ekRsTudp8ukkn95kiwg/wDrV7bDpemRarPq0Wn2kd/cII5rpYlEsijorNjJA7A0+2sLC0urq7trO2guLtle5ljjCvMwGAXI5YgcDNTLFXcmlul/X3XHHD25bvY5f41LG/wr8QRS6/F4fWS18v8AtCQsEhJZQMlfmw33TjnDV55+zjd6Ra67r3hy00XTbS/gsYJ57vSNXe9sblCWAYBifLkJOSOpH0r2PT9R0nXrW9jtnivreG4lsrkNGShkQ7ZE+YYbB4OMjII7GoPDem+GtMW7tvDthpNmqzbLqOwijTEuAcOE6NhgcHnBHrUQrKNJ031LnScqimuh85fs02ekyaloN/c6d4cN551wUvX8SSfby+ZAo+x52+3+781ZnxaNvF4z+Jl7P4dvbyaG+tYLbWY9RaCPSZJIVCu4Vsld2DnBAxzjNfTVj4L8HWN/Ff2PhXQ7W7hbdHPDYRJIjeoYDIPJo1iPwhYvdQ6tHols2sBvtKXKxqbwRoSxcN98KikknIAFdP11e1c0nr/nc5/qb9moX2/ysecfFf7b4U03wV8SGuWv5vD/AJVrrEsLbhdWkyKkj+jfPhgf9rNYJs71f2YvG/i7Uy6an4nt7jVZcscxxsQIUHoAmD/wKvWptZ8CSeFBDLd6M+hH/QhbsqmE7V3eV5eOy/NtxwvPTmp7nWPBV1LD4SuNR0O4e8t0EWltJG/nQsuVxH3QquRxjA9KxjiLRSts/wAN7Gzo3k3fp+O1zy/4qXFs9p8NNH8SajPp/g7UIwurTJM0SSSLboYYpJByqMc9+fwyNfTbLwHpnw28fWvgDWPtdrFYzm4ghv3uIbWT7MxAjJJC5HJwTz9MV6jeaZp17pp028sLW5smUIbaWFXiKjoNpGMCorDQtGsNKfSbHSbC1051ZXtYbdUiYMMMCoGDkdfWp9uuRR10+7e/3j9g+Zvv9+1jwf8AZjstHS+0y9/s/wANw6g+kZW5tfEklzeTMQm7zLUnEeRknH3SMV9E1haR4P8ACmjXq32keGdG0+6VSontrGOOQA9RuUA4NbtTiKqqz5kXQpOlDlYUUUVgbCGig9KKABaWkXpS0AFFFFABRRRQAUUUUAFFFFABRRRQAhopaKAEzxXi/wAWbzXtY+IcWm+Go7q6uPD2nNeJBEY2BvZWRAxSQYfy7eSVtpYAmRBkZr2mqFto+l2ur3mr29hbxX98qLdXCp+8lCDChj7Dj8qqMuV3Jkrqx4smnfGa8iuLKG613SEF3IIrie6t53IlmSONtwzmOKGN5m6FnlCLhRxR1XTfiprunT6VrOi65LZyu6mV3tHmSKe8Ky7PmHzJbKEUDbkTyk8AKfoUKPSo7iCKeCSCZFeKRCjoejKRgj8jVe08ifZ+Z4t8EdZ8Vap4ku7I2c1vpGji4jnhFzG0C3Esm6OBXTPmCOFYzkHlp3c4+VarOnxxFlBqdqL97qeJnuNPnaBVju1gmPykMdtsZWiQIMs/lZON5NezeH9E0vQNNTTtHs47S1RiwRMnLHqSSSWPuSTwPSr+0UOet7AoabnhuoaZ8ZC0zwahrSW2LUokdxAblzI0Mcmc/IvlRxSSNg4Z7ggEhTUN0/xuuEubn+zdUSa7S5aW1huYI44JY95t4onLZEbZUNIoBYRgYy7MPedo9KNoo9p5B7PzPJrzT/GGieDvCGjeGdE1AyWk8NxeRC4iXz9sgaVbiXf8hfc8jbQwLkLkjO7ndOtfjMsdnqUdhfQTm6ja5sg9tELqVYZJpZZiDxG8jQ2y8lhHFuPOK97wPSjaO1JTt0Dk8zxSyh+K1zqejQk+Ibeymjiur2e5kt1f7UGTzYyFY+VBgNhfnzuOASFzqfEXwfceJ/Eus6jeaTeCC006Cx0t7OKF5rmV5Vmlc7yB5Y8uKLDkDBl7HNer4FGBRzu90Pk0sz571vwH49nlR7lZrrW5r9dV+12rqtu8s5EFzbyyZDJAlpGiDaNzk7gcgCu88E6VrFv8Qbi+07T9W0jw9LaOb221JoyHu8xpCsCKSUSOKMqSDtIKAA4Jr0jAowKHUbVgVNJgKWiioLCiiigAooooAQ9KKD0ooAF6UtIvSloAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKAEbpRQ3SikwBaWmUuTimA6ikzxSA0AOopM0mTigB1FJmjNAC0U3JzQSaAHUU0E0ZNADqKaCaM0AOopueaXNAC0U3JoJoEOopM0maBjqKbuNLmgBaKbuNLmgBaKTNJmgB1FNzRk0AOpKQE0uaBC0UmaM0DFopuTQTzQAMaKSigD//2Q==";
    try {
      const logoBytes = Uint8Array.from(atob(LOGO_B64), c => c.charCodeAt(0));
      const logoImg = await pdf.embedJpg(logoBytes);
      // Draw logo at natural aspect ratio, fitted into left portion of header
      const logoW = 160;
      const logoH = 80;
      page.drawImage(logoImg, { x: margin, y: height - 88, width: logoW, height: logoH });
    } catch (_) {
      // Fallback if image fails to embed — draw text instead
      page.drawText("Davis Delivery Service, Inc.", { x: margin, y: height - 40, size: 18, font: fontBold, color: rgb(1, 1, 1) });
    }
    page.drawText("Payment Discrepancy Claim", { x: width - margin - 160, y: height - 38, size: 13, font: fontBold, color: rgb(1, 1, 1) });
    page.drawText("Uline Billing Audit", { x: width - margin - 160, y: height - 57, size: 9, font, color: rgb(0.85, 0.92, 1) });
    page.drawText(new Date().toLocaleDateString("en-US", { year:"numeric", month:"long", day:"numeric" }), { x: width - margin - 160, y: height - 72, size: 9, font, color: rgb(0.85, 0.92, 1) });
    y = height - 110;

    // Company info block
    page.drawText("Davis Delivery Service, Inc.", { x: margin, y, size: 10, font: fontBold, color: DARK });
    y -= 14;
    page.drawText("943 Gainesville Hwy, Buford, GA 30518", { x: margin, y, size: 9, font, color: MUTED });
    y -= 12;
    page.drawText("customerservice@davisdelivery.com | (770) 555-0100", { x: margin, y, size: 9, font, color: MUTED });
    y -= 20;

    // To section (date is now in header)

    page.drawText("To:", { x: margin, y, size: 10, font: fontBold, color: DARK });
    y -= 13;
    page.drawText(apContact.ap_contact_name || "Accounts Payable", { x: margin + 15, y, size: 10, font, color: DARK });
    y -= 12;
    // The dispute goes to Uline AP — the end customer is the delivery recipient,
    // not the party being billed. "customer" here is the end customer name which
    // belongs in the line item detail, not the addressee block.
    page.drawText("Uline", { x: margin + 15, y, size: 10, font, color: DARK });
    y -= 12;
    page.drawText("Attn: AP / Carrier Remittance", { x: margin + 15, y, size: 9, font, color: MUTED });
    if (apContact.billing_email) {
      y -= 12;
      page.drawText(apContact.billing_email, { x: margin + 15, y, size: 9, font, color: MUTED });
    }
    y -= 12;
    // Show what Uline customer this claim covers
    page.drawText(`Re: Deliveries to ${customer}`, { x: margin + 15, y, size: 9, font, color: MUTED });
    y -= 24;

    // Summary
    const totalBilled = items.reduce((s: number, i: any) => s + (i.billed || 0), 0);
    const totalPaid = items.reduce((s: number, i: any) => s + (i.paid || 0), 0);
    const totalClaim = totalBilled - totalPaid;

    page.drawText("CLAIM SUMMARY", { x: margin, y, size: 11, font: fontBold, color: BRAND });
    y -= 16;
    const sumLines = [
      `Total Invoiced: $${fmt(totalBilled)}`,
      `Total Paid: $${fmt(totalPaid)}`,
      `Amount Claimed: $${fmt(totalClaim)}  (${items.length} item${items.length > 1 ? "s" : ""})`,
    ];
    for (const line of sumLines) {
      page.drawText(line, { x: margin + 10, y, size: 10, font, color: DARK });
      y -= 14;
    }
    y -= 10;

    // Line items table
    page.drawText("LINE ITEM DETAIL", { x: margin, y, size: 11, font: fontBold, color: BRAND });
    y -= 18;

    // Table header
    const colX = [margin, margin + 85, margin + 175, margin + 285, margin + 345, margin + 410, margin + 470];
    const headers = ["PRO", "Pickup Date", "Category", "Billed", "Paid", "Variance", "Age"];
    page.drawRectangle({ x: margin - 5, y: y - 4, width: width - margin * 2 + 10, height: 16, color: rgb(0.95, 0.97, 1) });
    for (let i = 0; i < headers.length; i++) {
      page.drawText(headers[i], { x: colX[i], y, size: 8, font: fontBold, color: BRAND });
    }
    y -= 16;

    // Rows (paginate if needed)
    for (const item of items) {
      if (y < 80) {
        page = pdf.addPage([612, 792]);
        y = height - 60;
      }
      const pro = truncate(String(item.pro || ""), 12);
      const pu = item.pu_date || "";
      const cat = truncate(CATEGORY_LABELS[item.category] || item.category || "", 16);
      const billed = "$" + fmt(item.billed || 0);
      const paid = "$" + fmt(item.paid || 0);
      const variance = "$" + fmt((item.billed || 0) - (item.paid || 0));
      const age = item.age_days != null ? `${item.age_days}d` : "—";

      page.drawText(pro, { x: colX[0], y, size: 8, font, color: DARK });
      page.drawText(pu, { x: colX[1], y, size: 8, font, color: DARK });
      page.drawText(cat, { x: colX[2], y, size: 8, font, color: DARK });
      page.drawText(billed, { x: colX[3], y, size: 8, font, color: DARK });
      page.drawText(paid, { x: colX[4], y, size: 8, font, color: DARK });
      page.drawText(variance, { x: colX[5], y, size: 8, font: fontBold, color: RED });
      page.drawText(age, { x: colX[6], y, size: 8, font, color: MUTED });
      y -= 12;
    }

    y -= 15;
    if (y < 180) {
      page = pdf.addPage([612, 792]);
      y = height - 60;
    }

    // Supporting documentation note
    page.drawText("SUPPORTING DOCUMENTATION", { x: margin, y, size: 11, font: fontBold, color: BRAND });
    y -= 15;
    const supportLines = [
      "• Original Uline billing records (available on request)",
      "• Signed proof of delivery (available on request)",
      "• Accessorial authorization (where applicable)",
      "• DDIS820 remittance file showing payment discrepancy",
    ];
    for (const line of supportLines) {
      page.drawText(line, { x: margin + 10, y, size: 9, font, color: DARK });
      y -= 13;
    }
    y -= 15;

    // Requested resolution
    page.drawText("REQUESTED RESOLUTION", { x: margin, y, size: 11, font: fontBold, color: BRAND });
    y -= 15;
    const resolution = `Please issue remittance for $${fmt(totalClaim)} within 30 days, or contact our AR office to discuss.`;
    page.drawText(resolution, { x: margin + 10, y, size: 9, font, color: DARK, maxWidth: width - margin * 2 - 20 });
    y -= 20;

    // Footer
    page.drawText("Thank you,", { x: margin, y, size: 10, font, color: DARK });
    y -= 14;
    page.drawText("Davis Delivery Service AR Team", { x: margin, y, size: 10, font: fontBold, color: DARK });
    y -= 12;
    page.drawText("customerservice@davisdelivery.com", { x: margin, y, size: 9, font, color: MUTED });

    // Page numbering footer on every page
    const pages = pdf.getPages();
    for (let i = 0; i < pages.length; i++) {
      const p = pages[i];
      p.drawText(`Page ${i + 1} of ${pages.length} — Generated by Davis MarginIQ`, {
        x: margin,
        y: 30,
        size: 7,
        font,
        color: MUTED,
      });
    }

    const pdfBytes = await pdf.save();
    const b64 = Buffer.from(pdfBytes).toString("base64");

    return json({
      data: b64,
      filename: `Dispute_${customer.replace(/[^a-z0-9]/gi, "_")}_${new Date().toISOString().slice(0, 10)}.pdf`,
      item_count: items.length,
      total_claim: totalClaim,
    });
  } catch (err: any) {
    return json({ error: err.message || "PDF generation failed" }, 500);
  }
};

const CATEGORY_LABELS: Record<string, string> = {
  paid_in_full: "Paid in Full",
  short_paid: "Short-paid",
  accessorial_ignored: "Accessorial Ignored",
  zero_pay: "Zero-pay",
  overpaid: "Overpaid",
  orphan: "Orphan",
};

function fmt(n: number): string {
  return Number(n || 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function truncate(s: string, max: number): string {
  if (!s) return "";
  return s.length > max ? s.substring(0, max - 1) + "…" : s;
}

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
